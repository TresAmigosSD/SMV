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

class SmvCDSGroupBy(val srdd: SchemaRDD, keys: Seq[Symbol]){
  import srdd.sqlContext._


  /**
   * apply CDS and create the self-joined SRDD 
   **/
  def applyCDS(smvCDS: SmvCDS): SchemaRDD = {
    def names = srdd.schema.fieldNames
    val keyO = keys.map{s => names.indexWhere(s.name == _)}

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

  def toBeAggregated(smvCDS: SmvCDS) = {
    smvCDS match {
      case NoOpCDS(_) => srdd
      case _ => applyCDS(smvCDS)
    }
  }

  def singleCDSGroupBy(smvCDS: SmvCDS)(aggregateExpressions: Seq[NamedExpression]): SchemaRDD = {
    val keyColsExpr = (keys ++ smvCDS.outGroupKeys).map(k => UnresolvedAttribute(k.name))
    val aggrExpr = keyColsExpr ++ aggregateExpressions
    toBeAggregated(smvCDS).groupBy(keyColsExpr: _*)(aggrExpr: _*)
  }
}
