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
import org.apache.spark.sql.catalyst.types._
import java.sql.Timestamp

case class SmvChunkFunc(para: Seq[Symbol], outSchema: Schema, eval: List[Seq[Any]] => List[Seq[Any]])

class SmvChunk(srdd: SchemaRDD, keys: Seq[Symbol], chunkFunc: SmvChunkFunc, isPlus: Boolean = true){

  import srdd.sqlContext._

  def toSchemaRDD: SchemaRDD = {
    val names = srdd.schema.fieldNames
    val keyOrdinals = keys.map{s => names.indexWhere(s.name == _)}
    val ordinals = chunkFunc.para.map{s => names.indexWhere(s.name == _)}
    val cf = chunkFunc // For serialization
    val isP = isPlus //For serialization

    val resRdd = srdd.
      map{r => (keyOrdinals.map{i => r(i)}, r)}.groupByKey.map{case (keys, rIter) => 
        val rlist = rIter.toList
        val input = rlist.map{r => ordinals.map{i => r(i)}}
        val v = cf.eval(input)
        if (isP) {
          require(rlist.size == v.size)
          rlist.zip(v).map{case (orig, added) =>
            orig.toSeq ++ added
          }
        } else {
          v.map{added =>
            keys ++ added
          }
        }
      }.flatMap{r => r.map(l => Row.fromSeq(l))}

    val schema = if (isP) 
      Schema.fromSchemaRDD(srdd) ++ chunkFunc.outSchema
    else
      Schema.fromSchemaRDD(srdd.select(keys.map{symbolToUnresolvedAttribute(_)} : _*)) ++ chunkFunc.outSchema

    srdd.sqlContext.applySchema(resRdd, schema.toStructType)
  }

}
