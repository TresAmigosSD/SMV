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
import org.apache.spark.sql.catalyst.analysis.UnresolvedAttribute

case class SmvChunkUDF(
  para: Seq[Symbol], 
  outSchema: Schema, 
  eval: List[Seq[Any]] => List[Seq[Any]]
)

class SmvChunk(val srdd: SchemaRDD, keys: Seq[Symbol]){
  import srdd.sqlContext._

  def names = srdd.schema.fieldNames
  def keyOrdinal = keys.map{s => names.indexWhere(s.name == _)}

  /**
   * apply CDS and create the self-joined SRDD 
   **/
  def applyCDS(smvCDS: SmvCDS): SchemaRDD = {
    val keyO = keyOrdinal // for parallelization 

    val eval = smvCDS.eval(srdd.schema)
    val outSchema = smvCDS.outSchema(srdd.schema)

    val selfJoinedRdd = srdd.
      map{r => (keyO.map{i => r(i)}, r)}.groupByKey.
      map{case (k, rIter) => 
        val rlist = rIter.toList
        eval(rlist)
      }.flatMap{r => r}

    srdd.sqlContext.applySchema(selfJoinedRdd, outSchema)
  }

  def singleCDSGroupBy(smvCDS: SmvCDS)(aggregateExpressions: Seq[NamedExpression]): SchemaRDD = {
    val toBeAggregated = smvCDS match {
      case NoOpCDS(_) => srdd
      case _ => applyCDS(smvCDS)
    }
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
