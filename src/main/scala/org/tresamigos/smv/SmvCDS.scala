/*
 * This file is licensed under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.tresamigos.smv

import org.apache.spark.sql.SchemaRDD
import org.apache.spark.sql.catalyst.analysis.UnresolvedAttribute
import org.apache.spark.SparkContext._
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans.logical.LocalRelation
import org.apache.spark.sql.catalyst.types._
import org.apache.spark.sql.catalyst.dsl.expressions._
import org.apache.spark.sql.catalyst.dsl.plans._
import org.apache.spark.sql.catalyst.plans.{Inner}

/**
 * SmvCDS - SMV Custom Data Seletor 
 *
 * As generalizing the idea of running sum and similar requirements, we define
 * an abstract class as CDS. Using the running sum as an example, the overall
 * client code will look like
 *
 * srdd.smvSingleCDSGroupBy('k)(TimeInLastN('t, 3))((Sum('v) as 'nv1), (Count('v) as 'nv2))
 *
 * where TimeInLastN('t, 3) is a concrete SmvCDS 
 *
 * Future could also implement custom Catalyst expressions to integrate SmvCDS into expressions. 
 * The client code looks like
 *
 * srdd.smvGroupBy('k)(
 *     Sum('v1) from TimeInLastN('t, 3) as 'nv1,
 *         Count('v2) from TimeInLastN('t, 6) as 'nv2
 *       )
 **/

abstract class SmvCDS {
  val outGroupKeys: Seq[Symbol]
  def createSrdd(srdd: SchemaRDD, keys: Seq[Symbol]): SchemaRDD
}

/** 
 *  NoOpCDS
 *  With it smvSingleCDSGroupBy will behave like SchemaRDD groupBy 
 *  smvSingleCDSGroupBy(keys)(NoOpCDS(more_keys))(...)  is the same as 
 *  groupBy(keys ++ more_keys)(keys ++ more_keys ++ ...)
 **/
case class NoOpCDS(outGroupKeys: Seq[Symbol]) extends SmvCDS{
  def createSrdd(srdd: SchemaRDD, keys: Seq[Symbol]): SchemaRDD = srdd
}


/** 
 *  SmvCDSOnRdd is a type of SmvCDS implementation which use the groupByKey
 *  method of Rdd to create an Iterable[Row] and send to the concrete class'
 *  eval function to get back an Iterable[Row]. 
 * 
 *  SmvCDSOnRdd by itself does not require in memory procession of the
 *  Iterable[Row]. However the concrete class of it may need to load
 *  Iterable[Row] to memory.
 **/
abstract class SmvCDSOnRdd extends SmvCDS {

  def outSchema(inSchema: StructType): StructType
  def eval(inSchema: StructType): Iterable[Row] => Iterable[Row]

  /**
   * Create the self-joined SRDD 
   **/
  def createSrdd(srdd: SchemaRDD, keys: Seq[Symbol]): SchemaRDD = {
    def names = srdd.schema.fieldNames
    val keyO = keys.map{s => names.indexWhere(s.name == _)}

    val evalrow = eval(srdd.schema)
    val outS = outSchema(srdd.schema)

    val selfJoinedRdd = srdd.
      map{r => (keyO.map{i => r(i)}, r)}.groupByKey.
      map{case (k, rIter) => 
        evalrow(rIter)
      }.flatMap(r => r)

    srdd.sqlContext.applySchema(selfJoinedRdd, outS)
  }
}

/**
 *  SmvCDSRange(outGroupKeys, condition)
 *
 *  Defines a "self-joined" data for further aggregation with this logic
 *  srdd.select(outGroupKeys).distinct.join(srdd, Inner, Option(condition)
 **/
case class SmvCDSRange(outGroupKeys: Seq[Symbol], condition: Expression) extends SmvCDSOnRdd {
  require(condition.dataType == BooleanType)

  def outSchema(inSchema: StructType) = {
    val renamed = inSchema.fields.map{f => 
      if (outGroupKeys.map{_.name}.contains(f.name)) StructField("_" + f.name, f.dataType, f.nullable)
      else f
    }
    //val added = inSchema.fields.collect{ case f if outGroupKeys.map{_.name}.contains(f.name) => f}
    val added = outGroupKeys.collect{case a if(inSchema.fields.map{_.name}.contains(a.name)) => inSchema(a.name)}
    StructType(added ++ renamed)
  }

  def eval(inSchema: StructType): Iterable[Row] => Iterable[Row] = {
    val attr = outSchema(inSchema).fields.map{f => AttributeReference(f.name, f.dataType, f.nullable)()}
    val aOrdinal = outGroupKeys.map{a => inSchema.fields.indexWhere(a.name == _.name)}
    val fPlan= LocalRelation(attr).where(condition).analyze
    val filter = BindReferences.bindReference(fPlan.expressions(0), attr)

    {it =>
      val f = filter
      val itSeq = it.toSeq
      val anchors = itSeq.map{r => aOrdinal.map{r(_)}}.distinct
      anchors.flatMap{ a => 
        itSeq.map{r => 
          new GenericRow((a ++ r).toArray)
        }.collect{case r: Row if f.eval(r).asInstanceOf[Boolean] => r}
      }
    }
  }
}

/**
 *  SmvCDSRangeSelfJoin(outGroupKeys, condition)
 *
 *  Defines a "self-joined" data for further aggregation with this logic
 *  srdd.select(keys ++ outGroupKeys).distinct.joinByKey(srdd, Inner,keys).where(condition)
 **/
case class SmvCDSRangeSelfJoin(outGroupKeys: Seq[Symbol], condition: Expression) extends SmvCDS {
  require(condition.dataType == BooleanType)

  def createSrdd(srdd: SchemaRDD, keys: Seq[Symbol]) = {
    val srdd_right = srdd.renameField(outGroupKeys.map{s => s -> Symbol("_" + s.name)}: _*)
    srdd.select((keys ++ outGroupKeys).map{_.attr}: _*).
      distinct.
      joinByKey(srdd_right, Inner, keys).where(condition)
  }
}


/**
 * TimeInLastNFromAnchor(t, anchor, n)
 *
 * Defince a self-join with condition: "t" in the last "n" from "anchor"
 * (t <= anchor && t > anchor - n)
 **/
object TimeInLastNFromAnchor {
  def apply(t: Symbol, anchor: Symbol, n: Int) = {
    val outGroupKeys = Seq(anchor)
    val condition = (t <= anchor && t > anchor - n)

    SmvCDSRange(outGroupKeys, condition)
  }
}

/** 
 * TimeInLastN(t, n)
 * 
 * Defince a self-join with "t" in the last "n" from the current "t"
 */
object TimeInLastN {
  def apply(t: Symbol, n: Int) = {
    val outGroupKeys = Seq(t)
    val withPrefix = Symbol("_" + t.name)
    val condition = (withPrefix <= t &&  withPrefix > t - n)

    SmvCDSRange(outGroupKeys, condition)
  }
}

/**
 *  SmvCDSTopRec is a SmvCDS to support smvTopRec(keys)(order) method, which
 *  logically equivalent to
 *  
 *  srdd.orderBy(order).groupBy(keys)(First(f1), First(f2) ...)
 *  
 *  But should be a lot more efficient, since the CDS implementation does not
 *  need sorting the entire Srdd. 
 *
 *  TODO: multiple order keys, and multiple output Records - SmvCDSTopNRecs
 *
 **/
case class SmvCDSTopRec(orderKey: SortOrder) extends SmvCDSOnRdd {
  val outGroupKeys = Nil
  def outSchema(inSchema: StructType) = inSchema

  private val keyCol = orderKey.child.asInstanceOf[NamedExpression].name

  private val cmpCurrentToTop =  
    if (orderKey.direction == Ascending) {
      If(IsNull('top), Literal(true), ('curr < 'top))
    } else {
      If(IsNull('top), Literal(true), ('curr > 'top))
    }



  def eval(inSchema: StructType): Iterable[Row] => Iterable[Row] = {
    val ordinal = inSchema.fields.indexWhere(keyCol == _.name)
    val col = inSchema(keyCol)

    val attr = Seq(
      AttributeReference("top", col.dataType, true)(),
      AttributeReference("curr", col.dataType, col.nullable)()
    )
    val fPlan= LocalRelation(attr).select(cmpCurrentToTop as 'newtop).analyze
    val isReplace = BindReferences.bindReference(fPlan.expressions(0), attr)

    {it => 
      var topVal: Any = null
      var res: Row = EmptyRow
      it.map{r =>
        val v = r(ordinal)
        if (isReplace.eval(new GenericRow(Array(topVal, v): Array[Any])).asInstanceOf[Boolean]) {
          topVal = v
          res = r
        }
      }
      if (res == EmptyRow) Seq()
      else(Seq(res))
    }
  }
}
