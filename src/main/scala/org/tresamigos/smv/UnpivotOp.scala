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
import org.apache.spark.sql.catalyst.expressions._


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
class UnpivotOp(val srdd: SchemaRDD, val valueCols: Seq[Symbol]) {
  // TODO: should not hardcode the column/value column names in the result.
  // TODO: perhaps accept multiple valueCols sets.

  /**
   * Creates the projection array expression required to unpivot the pivoted data:
   * Given value columns (X,Y,Z), this produces:
   * Array( Array("X", 'X), Array("Y", 'Y), Array("Z", 'Z) )
   */
  private def unpivotExplodeArray() = {
    SmvAsArray(valueCols.
      map(_.name).
      map(vn => SmvAsArray(Literal(vn), UnresolvedAttribute(vn))): _*
    )
  }

  def unpivot() = {
    import srdd.sqlContext._

    srdd.
      generate(Explode(Seq("_unpivot_vals"), unpivotExplodeArray()), true).
      selectMinus(valueCols: _*).
      selectPlus(
        GetItem("_unpivot_vals".attr, 0) as 'column,
        GetItem("_unpivot_vals".attr, 1) as 'value).
      selectMinus(Symbol("_unpivot_vals"))
  }
}
