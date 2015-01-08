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
import org.apache.spark.sql._
//import org.apache.spark.sql.catalyst.types._
import org.apache.spark.sql.catalyst.expressions.{Expression, NamedExpression}
import java.sql.Timestamp
import scala.reflect.ClassTag

abstract class SmvChunkRange extends Serializable{
  def eval(value: Any, anchor: Any): Boolean
}

case class InLastN[T:Numeric](n: T) extends SmvChunkRange{
  def eval(curr: Any, anchor: Any) = {
    val a = anchor.asInstanceOf[T]
    val v = curr.asInstanceOf[T]
    val num = implicitly[Numeric[T]]
    num.lteq(v, a) && num.gt(v, num.minus(a, n))
  }
  override def toString = s"InLast$n"
}

abstract class SmvChunkFunc{
  val para: Seq[Symbol]
}

case class SmvChunkUDF(
  para: Seq[Symbol], 
  outSchema: Schema, 
  eval: List[Seq[Any]] => List[Seq[Any]]
) extends SmvChunkFunc 

case class RunSum(value: NamedExpression, time: NamedExpression, cond: SmvChunkRange) extends SmvChunkFunc{
  override def toString = "RunSum_" + cond.toString + "_" + time.name + "_on_" + value.name

  val para = Seq(Symbol(time.name), Symbol(value.name))

  def evalWithSEntry(l: List[Seq[Any]], se: NumericSchemaEntry): List[Seq[Any]] = { 
    val nv = se.numeric
    l.map{ r => 
      val anchor = r(0)
      val res = l.map{ rr => 
        if(cond.eval(rr(0), anchor)) rr(1).asInstanceOf[se.JvmType]
        else nv.zero
      }.reduceLeft{nv.plus(_, _)}
      Seq(res)
    }
  }
}


class SmvChunk(val srdd: SchemaRDD, keys: Seq[Symbol]){
  import srdd.sqlContext._

  def names = srdd.schema.fieldNames
  def keyOrdinals = keys.map{s => names.indexWhere(s.name == _)}

  def applyUDF(chunkFunc: SmvChunkFunc, isPlus: Boolean = true): SchemaRDD = {
    val keyO = keyOrdinals // for parallelization 
    val ordinals = chunkFunc.para.map{s => names.indexWhere(s.name == _)}

    val (eval, addedSchema) = chunkFunc match {
      case f: SmvChunkUDF => 
        (f.eval, f.outSchema)
      case s: RunSum =>
        val se = NumericSchemaEntry(s.toString, srdd.schema(s.value.name).dataType)
        val e: List[Seq[Any]] => List[Seq[Any]] = {l => s.evalWithSEntry(l, se)}
        val as = new Schema(Seq(se))
        (e, as)
      case _ => throw new IllegalArgumentException(s"Unsupported SmvChunkFunc")
    }

    val resRdd = srdd.
      map{r => (keyO.map{i => r(i)}, r)}.groupByKey.
      map{case (k, rIter) => 
        val rlist = rIter.toList
        val input = rlist.map{r => ordinals.map{i => r(i)}}
        val v = eval(input)
        if (isPlus) {
          require(rlist.size == v.size)
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
