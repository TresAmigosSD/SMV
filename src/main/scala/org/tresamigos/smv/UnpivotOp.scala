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

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._


/**
 * Almost the opposite of the pivot operation.  Given a set of records with value columns,
 * turns the value columns into value rows.  For example:
 * {{{
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
 * }}}
 *
 * This only works for String columns for now (due to limitation of Explode method)
 */
private[smv] class UnpivotOp(val df: DataFrame, val valueCols: Seq[String]) {
  import df.sqlContext.implicits._

  // TODO: should not hardcode the column/value column names in the result.
  // TODO: perhaps accept multiple valueCols sets.

  val types = valueCols.map{c => df(c).toExpr.dataType}.toSet.toSeq
  require(types.size == 1)
  val dataType = types(0)

  /**
   * Creates the projection array expression required to unpivot the pivoted data:
   * Given value columns (X,Y,Z), this produces:
   * Array( Struct("X", 'X), Struct("Y", 'Y), Struct("Z", 'Z) )
   */

  val arrayCol = array(valueCols.map{c => struct(lit(c) as "column", $"${c}" as "value")}: _*)

  def unpivot() = {
    df.selectPlus(arrayCol as "_unpivot_vals").
      selectPlus(explode($"_unpivot_vals") as "_kvpair").
      selectPlus($"_kvpair".getField("column") as "column", $"_kvpair".getField("value") as "value").
      selectMinus($"_unpivot_vals", $"_kvpair").
      selectMinus(valueCols.head, valueCols.tail: _*)
  }
}
