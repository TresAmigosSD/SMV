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

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SchemaRDD
import org.apache.spark.SparkContext._
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans.logical.LocalRelation
import org.apache.spark.sql.catalyst.types._
import org.apache.spark.sql.catalyst.dsl.expressions._
import org.apache.spark.sql.catalyst.dsl.plans._
import org.apache.spark.sql.catalyst.analysis.UnresolvedAttribute

abstract class SmvCDS {
  val outGroupKeys: Seq[Symbol]
  def outSchema(inSchema: StructType): StructType
  def eval(inSchema: StructType): Seq[Row] => Seq[Row]
}

case class SmvCDSRange(outGroupKeys: Seq[Symbol], condition: Expression) extends SmvCDS{
  require(condition.dataType == BooleanType)

  def outSchema(inSchema: StructType) = {
    val renamed = inSchema.fields.map{f => 
      if (outGroupKeys.map{_.name}.contains(f.name)) StructField("_" + f.name, f.dataType, f.nullable)
      else f
    }
    val added = inSchema.fields.collect{ case f if outGroupKeys.map{_.name}.contains(f.name) => f}
    StructType(added ++ renamed)
  }

  def eval(inSchema: StructType): Seq[Row] => Seq[Row] = {
    val attr = outSchema(inSchema).fields.map{f => AttributeReference(f.name, f.dataType, f.nullable)()}
    val aOrdinal = outGroupKeys.map{a => inSchema.fields.indexWhere(a.name == _.name)}
    val fPlan= LocalRelation(attr).where(condition).analyze
    val filter = BindReferences.bindReference(fPlan.expressions(0), attr)

    {it =>
      val f = filter
      val anchors = it.toSeq.map{r => aOrdinal.map{r(_)}}.distinct
      anchors.flatMap{ a => 
        it.toSeq.map{r => 
          new GenericRow((a ++ r).toArray)
        }.collect{case r: Row if f.eval(r).asInstanceOf[Boolean] => r}
      }
    }
  }
}

object TimeInLastNFromAnchor {
  def apply(t: Symbol, anchor: Symbol, n: Int) = {
    val outGroupKeys = Seq(anchor)
    val condition = (t <= anchor && t > anchor - n)

    SmvCDSRange(outGroupKeys, condition)
  }
}

object TimeInLastN {
  def apply(t: Symbol, n: Int) = {
    val outGroupKeys = Seq(t)
    val withPrefix = Symbol("_" + t.name)
    val condition = (withPrefix <= t &&  withPrefix > t - n)

    SmvCDSRange(outGroupKeys, condition)
  }
}

case class SmvChunkUDF(
  para: Seq[Symbol], 
  outSchema: Schema, 
  eval: List[Seq[Any]] => List[Seq[Any]]
)

class SmvChunk(val srdd: SchemaRDD, keys: Seq[Symbol]){
  import srdd.sqlContext._

  def names = srdd.schema.fieldNames
  def keyOrdinal = keys.map{s => names.indexWhere(s.name == _)}

  def applyCDS(smvCDS: SmvCDS)(aggregateExpressions: Seq[NamedExpression]): SchemaRDD = {
    val keyO = keyOrdinal // for parallelization 

    val eval = smvCDS.eval(srdd.schema)
    val outSchema = smvCDS.outSchema(srdd.schema)

    val selfJoinedRdd = srdd.
      map{r => (keyO.map{i => r(i)}, r)}.groupByKey.
      map{case (k, rIter) => 
        val rlist = rIter.toList
        eval(rlist)
      }.flatMap{r => r}

    val toBeAggregated = srdd.sqlContext.applySchema(selfJoinedRdd, outSchema)

    val keyColsExpr = (keys ++ smvCDS.outGroupKeys).map(k => UnresolvedAttribute(k.name))
    val aggrExpr = keyColsExpr ++ aggregateExpressions
    toBeAggregated.groupBy(keyColsExpr: _*)(aggrExpr: _*)
  }

  def applyUDF(chunkFuncs: Seq[SmvChunkUDF], isPlus: Boolean = true): SchemaRDD = {
    val keyO = keyOrdinal // for parallelization 
    val ordinals = chunkFuncs.map{l => l.para.map{s => names.indexWhere(s.name == _)}}

    val eval = chunkFuncs.map{_.eval}
    val addedSchema = chunkFuncs.map{_.outSchema}.reduce(_ ++ _)

    val resRdd = srdd.
      map{r => (keyO.map{i => r(i)}, r)}.groupByKey.
      map{case (k, rIter) => 
        val rlist = rIter.toList
        val input = ordinals.map{o => rlist.map{r => o.map{i => r(i)}}}
        val v = eval.zip(input).map{case (f, in) => f(in)}.transpose.map{a => a.flatMap{b => b}}
        if (isPlus) {
          rlist.size == v.size
          rlist.zip(v).map{case (orig, added) =>
            orig.toSeq ++ added
          }
        }else{
          v.map{added =>
            k ++ added
          }
        }
      }.flatMap{r => r.map(l => Row.fromSeq(l))}

    val schema = if (isPlus) 
      Schema.fromSchemaRDD(srdd) ++ addedSchema
    else
      Schema.fromSchemaRDD(srdd.select(keys.map{symbolToUnresolvedAttribute(_)} : _*)) ++ addedSchema

    srdd.sqlContext.applySchema(resRdd, schema.toStructType)
  }

}
