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
import org.apache.spark.sql.catalyst.types.StringType

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
class PivotOp(origSRDD: SchemaRDD,
              keyCol: Symbol,
              pivotCols: Seq[Symbol],
              valueCols: Seq[Symbol]) {
  // TODO: allow multiple key columns.
  // TODO: allow multiple value columns.

  import origSRDD.sqlContext._

  val baseOutputColumnNames = getBaseOutputColumnNames()
  val tempPivotValCol = '_smv_pivot_val
  val keyColExpr = UnresolvedAttribute(keyCol.name)
  val pivotColsExpr = pivotCols.map(s => UnresolvedAttribute(s.name))
  val valueColsExpr = valueCols.map(v => UnresolvedAttribute(v.name))

  /**
   * Extract the column names from the data.
   * This is done by getting the distinct string values of each column and taking the cartesian product.
   * For N value columns, the output shall have N times as many columns as the returned list as
   * each item in the list would have the value column name(s) prepended.
   * Return: Seq["5_14_A", "5_14_B", ...]
   */
  private[smv] def getBaseOutputColumnNames(): Seq[String] = {
    // create set of distinct values.
    // this is a seq of array strings where each array is distinct values for a column.
    val distinctVals = pivotCols.map(s => origSRDD.select(s).
      distinct.collect.map { r =>
        Option(r(0)).getOrElse("").toString
      })

    // get the cartesian product of all column values.
    val colNames = distinctVals.reduceLeft(
      (l1,l2) => for(v1 <- l1; v2 <- l2) yield (v1 + "_" + v2))

    // ensure result column name is made up of valid characters.
    colNames.map(c => SchemaEntry.valueToColumnName(c))
  }

  /**
   * Create a derived column that contains the concatenation of all the values in
   * the pivot columns.  From the above columns, create a new column with following values:
   * | key | _smv_pivot_val | count |
   * | --- | -------------- | ----- |
   * |  1  | count_5_14_A   |   100 |
   * |  1  | count_5_14_B   |   200 |
   * |  1  | count_6_14_A   |   300 |
   * |  1  | count_6_14_B   |   400 |
   */
  private[smv] def addSmvPivotValColumn() : SchemaRDD = {
    origSRDD.select(
      keyColExpr +:
      (SmvPivotVal(pivotColsExpr) as tempPivotValCol) +:
      valueColsExpr: _*)
  }

  /**
   * Map the output column to the corresponding output column Given the output from addSmvPivotValColumn.
   * The expected output is:
   * | key | count_5_14_A | count_5_14_B | ... |
   * | --- | ------------ | ------------ | --- |
   * |  1  |     100      |       0      |  0  |
   * |  1  |       0      |     300      |  0  |
   * |  1  |       0      |       0      |  0  |
   * |  1  |       0      |       0      |  0  |
   */
  private[smv] def mapValColsToOutputCols(srddWithPivotValCol: SchemaRDD) = {
    import srddWithPivotValCol.sqlContext._

    // find zero value to match type of valueCol.  If type is mismatched, then we get
    // a very weird attribute not resolved error.
    val schema = Schema.fromSchemaRDD(srddWithPivotValCol)
    val valueColsZeroVals = valueCols.map(vc => schema.findEntry(vc).get.zeroVal)

    val outputColExprs = valueCols.zip(valueColsZeroVals).map { case (vcSymbol, vcZero) =>
      baseOutputColumnNames.map { outCol =>
        If(tempPivotValCol === outCol, vcSymbol, vcZero) as Symbol(vcSymbol.name + "_" + outCol)
      }
    }.flatten
    srddWithPivotValCol.select(keyColExpr +: outputColExprs: _*)
  }

  /**
   * Perform the actual pivot transformation.
   * WARNING: this should not be called directly by user.  User should use the pivot_sum method.
   */
  def transform = {
    val outColSumExprs = valueCols.map {vc =>
      baseOutputColumnNames.map { c =>
        val colName = vc.name + "_" + c
        Sum(colName.attr) as Symbol(colName)
      }
    }.flatten
    val keyColExpr = UnresolvedAttribute(keyCol.name)

    mapValColsToOutputCols(addSmvPivotValColumn).
      groupBy(keyColExpr)(keyColExpr +: outColSumExprs: _*)
  }
}

/**
 * Expression that evaluates to a string concat of all pivot column (children) values.
 * WARNING: this must be at the module top level and not embedded inside a function def
 * as it would cause an exception during tree node copy.
 */
private [smv] case class SmvPivotVal(children: Seq[Expression])
  extends Expression {
  override type EvaluatedType = Any
  override def dataType = StringType
  override def nullable = true
  override def toString = s"smvPivotVal(${children.mkString(",")})"

  // concat all the children (pivot columns) values to form a single value
  override def eval(input: Row): Any = {
    SchemaEntry.valueToColumnName(
      children.map { c =>
        val v = c.eval(input)
        if (v == null) "" else v.toString
      }.mkString("_"))
  }
}
