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
import org.apache.spark.sql.{ColumnName}
import org.apache.spark.sql.catalyst.analysis.UnresolvedAttribute
import org.apache.spark.sql.catalyst.expressions.{Alias, First, Literal, Expression}

/**
 * implement the cube/rollup operations on a given SRDD and a set of columns.
 * See http://joshualande.com/cube-rollup-pig-data-science/ for the pig implementation.
 * Rather than using nulls as the pig version, a sentinel value of "*" will be used
 */
class RollupCubeOp(df: DataFrame,
                   keyCols: Seq[String],
                   cols: Seq[String]) {

  /** for N cube cols, we want to produce 2**N columns (minus all "*") */
  def cubeBitmasks() = {
    Seq.tabulate((1 << cols.length)-1)(i => i)
  }

  /** for N rollup cols, we want to produce N-1 sentinel columns */
  def rollupBitmasks() = {
    Seq.tabulate(cols.length)(i => (1 << i) - 1)
  }

  /** return list of non-rollup/cube columns in the given srdd. */
  def getNonRollupCols() = {
    df.schema.fieldNames.filterNot(n => cols.contains(n))
  }

  /**
   * creates a new SRDD with values from the cols substituted by the sentinel value "*"
   * based on the bitmask value.
   */
  def createSRDDWithSentinel(bitmask: Int) = {
    import df.sqlContext.implicits._

    val cubeColsSelect = cols.zipWithIndex.map { case (s, i) =>
      val idx = cols.length - i - 1
      if (((1 << idx) & bitmask) != 0) lit("*") as s else $"$s"
    }
    val otherColsSelect = getNonRollupCols().map(n => $"$n")

    df.select(cubeColsSelect ++ otherColsSelect: _*)
  }

  /**
   * duplicates the input SRDD into a single union of multiple copies where each copy is
   * a variation of the original input with sentinel columns replaced with "*" based on
   * the given bitmasks.
   */
  def duplicateSRDDByBitmasks(bitmasks: Seq[Int]) = {
    bitmasks.map(m => createSRDDWithSentinel(m)).reduceLeft((s1,s2) => s1.unionAll(s2))
  }

  /**
   * perform the groupBy operation on the duplicated data set.
   */
  private def duplicateAndGroup(bitmasks: Seq[Int]) = {
    val cubeCols = (keyCols ++ cols).map(n => new ColumnName(n))
    duplicateSRDDByBitmasks(bitmasks).
      groupBy(cubeCols: _*)
  }

  /**
   * perform the cube operation on the SRDD and cube columns.
   */
  def cube() = duplicateAndGroup(cubeBitmasks())

  /**
   * perform the rollup operation on the SRDD and rollup columns.
   */
  def rollup() = duplicateAndGroup(rollupBitmasks())
}
