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
import org.apache.spark.sql.ColumnName
import org.apache.spark.sql.functions._
import org.apache.spark.sql.catalyst.analysis.UnresolvedAttribute
import org.apache.spark.sql.catalyst.expressions._
import scala.reflect.runtime.universe.{TypeTag, typeTag}


/**
 * Almost the oppisite of the pivot operation.  Given a set of records with value columns,
 * turns the value columns into value rows.  For example:
 * | id | X | Y | Z |
 * | -- | - | - | - |
 * | 1  | A | B | C |
 * | 2  | D | E | F |
 * | 3  | G | H | I |
 *
 * The above would unpivot to:
 * | id | column | value |
 * | -- | ------ | ----- |
 * |  1 |   X    |   A   |
 * |  1 |   Y    |   B   |
 * |  1 |   Z    |   C   |
 * | ...   ...      ...  |
 * |  3 |   Z    |   I   |
 *
 * This only works for String columns for now (due to limitation of Explode method)
 */
class UnpivotOp(val srdd: SchemaRDD, val valueCols: Seq[String]) {
  // TODO: should not hardcode the column/value column names in the result.
  // TODO: perhaps accept multiple valueCols sets.
  
  val types = valueCols.map{c => srdd(c).toExpr.dataType}.toSet.toSeq
  require(types.size == 1)
  val dataType = types(0)
  
  val dummyEntry = SchemaEntry("dummy", dataType).asInstanceOf[NativeSchemaEntry]
  
  /**
   * Creates the projection array expression required to unpivot the pivoted data:
   * Given value columns (X,Y,Z), this produces:
   * Array( Array("X", 'X), Array("Y", 'Y), Array("Z", 'Z) )
   */
  private def unpivotExplodeArray() = {
    smvAsArray(valueCols.
      map(vn => smvAsArray(lit(vn), srdd(vn))): _*
    )
  }

  def unpivot() = {
    import srdd.sqlContext.implicits._

    srdd.selectPlus(unpivotExplodeArray() as "_unpivot_vals").
      explode("_unpivot_vals", "_kvpair")((a: Seq[Seq[String]]) => a).
      selectPlus($"_kvpair".getItem(0) as 'column, $"_kvpair".getItem(1) as 'value).
      selectMinus("_unpivot_vals", "_kvpair").
      selectMinus(valueCols: _*)
  }
}
