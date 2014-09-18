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

/**
 * Pivot operation on SchemaRDD that transforms multiple rows per key into a single row for
 * a given key while preserving all the data variance by turning row values into columns.
 * For Example:
 * | id  | month | product | count |
 * | --- | ----- | ------- | ----- |
 * | 1   | 5/14  |   A     |   100 |
 * | 1   | 6/14  |   B     |   200 |
 * | 1   | 5/14  |   B     |   300 |
 * 
 * We would like to generate a single row for each unique id but still maintain the full granularity of the data.
 * The desired output is:
 * 
 * | id  | count_5_14_A | count_5_14_B | count_6_14_A | count_6_14_B |
 * | --- | ------------ | ------------ | ------------ | ------------ |
 * | 1   | 100          | 300          | 0            | 200          |
 * 
 * The raw input is divided into three parts.
 * 1. key column: part of the primary key that is preserved in the output.
 *    That would be the `id` column in the above example.
 * 2. pivot columns: the columns whose row values will become the new column names.
 *    The cross product of all unique values for *each* column is used to generate the output column names.
 * 3. value column: the value that will be copied/aggregated to corresponding output column. `count` in our example.
 * 
 */
class PivotOp(origSRDD: SchemaRDD) {
  // TODO: add upper limit on number of columns for pivot op.
  // TODO: allow multiple key columns.
  // TODO: allow multiple value columns.

  /**
   * Extract the column names from the data.
   * This is done by getting the distinct string values of each column and taking the cartesian product.
   * The value column name is then prepended to create the final column names.
   * Return: Seq["count_5_14_A", "count_5_14_B", ...]
   */
  private[smv] def getFlatColumnNames(pivotCols: Seq[Symbol], valueCol: Symbol) = {
    import origSRDD.sqlContext._

    // create set of distinct values.
    // this is a seq of array strings where each array is distinct values for a column.
    val distinctVals = pivotCols.map(s => origSRDD.select(s).
      distinct.collect.map { r =>
        Option(r(0)).getOrElse("").toString
      })

    // get the cartesian product of all column values.
    val colNames = distinctVals.reduceLeft(
      (l1,l2) => for(v1 <- l1; v2 <- l2) yield (v1 + "_" + v2))

    // prepend value column name to each column and ensure result column name is valid.
    colNames.map(v => SchemaEntry.valueToColumnName(valueCol.name + "_" + v))
  }

  def addColNameValue() = {
  }

  def pivot_sum(keyCol: Symbol, pivotCols: Seq[Symbol], valueCol: Symbol) = {
    //getFlatColumnNames(pivotCols, valueCol)

  }
}
