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

import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions._, codegen.{CodeGenContext,GeneratedExpressionCode}
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._

/**
 * SmvPivot for Pivot Operations:
 *
 * Pivot Operation on DataFrame that transforms multiple rows per key into a single row for
 * a given key while preserving all the data variance by turning row values into columns.
 * For Example:
 * | id  | month | product | count |
 * | --- | ----- | ------- | ----- |
 * | 1   | 5/14  |   A     |   100 |
 * | 1   | 6/14  |   B     |   200 |
 * | 1   | 5/14  |   B     |   300 |
 *
 * We would like to generate a data set to be ready for aggregations.
 * The desired output is:
 *
 * | id  | count_5_14_A | count_5_14_B | count_6_14_A | count_6_14_B |
 * | --- | ------------ | ------------ | ------------ | ------------ |
 * | 1   | 100          | NULL         | NULL         | NULL         |
 * | 1   | NULL         | NULL         | NULL         | 200          |
 * | 1   | NULL         | 300          | NULL         | NULL         |
 *
 * The raw input is divided into three parts.
 * 1. key column: part of the primary key that is preserved in the output.
 *    That would be the `id` column in the above example.
 * 2. pivot columns: the columns whose row values will become the new column names.
 *    The cross product of all unique values for *each* column is used to generate the output column names.
 * 3. value column: the value that will be copied/aggregated to corresponding output column. `count` in our example.
 *
 * @param pivotColSets specify the pivot columns, on above example, it is
 *        Seq(Seq('month, 'product)). If Seq(Seq('month), Seq('month,'product)) is
 *        used, the output columns will have "count_5_14" and "count_6_14" as
 *        addition to the example.
 * @param valueColPrefixMap specify the value column and how it will be used
 *        to create the output columns. In above example, it is Seq(('count, "count")).
 *        If we use Seq(('count, "num")) instead, the output column names will
 *        be "num_5_14_A" etc.
 * @param baseOutputColumnNames specify the base names of the output columns.
 *        In above example, it is Seq("5_14_A", "5_14_B", "6_14_A", "6_14_B").
 *        We assume that the output columns are predefined here. In case that
 *        we truly want to have a data driven pivoting, we can call a helper
 *        function, PivotOp.getBaseOutputColumnNames(df, Seq(pivotCols))
 *        to generate baseOutputColumnNames.
 */

private[smv] case class SmvPivot(
        pivotColSets: Seq[Seq[String]],
        valueColPrefixMap: Seq[(String, String)],
        baseOutputColumnNames: Seq[String]) {
  require(!baseOutputColumnNames.isEmpty, "Pivot requires the output column names to be known")

  def createSrdd(df: DataFrame, keys: Seq[String]): DataFrame =
    mapValColsToOutputCols(addSmvPivotValColumn(df), keys.map{k => new ColumnName(k)})

  private val tempPivotValCol = "_smv_pivot_val"

  private val createColName = (prefix: String, baseOutCol: String) => prefix + "_" + baseOutCol
  private val contains: (Seq[Any], Any) => Boolean = { (a, v) => a.contains(v) }

  private val outputColExprs = valueColPrefixMap.map {case (valueCol, prefix) =>
    //  Zero filling is replaced by Null filling to handle CountDistinct right
    baseOutputColumnNames.map { outCol =>
      when(array_contains(new ColumnName(tempPivotValCol), outCol), new ColumnName(valueCol)).
        otherwise(null) as createColName(prefix, outCol)
    }
  }.flatten

  def outCols(): Seq[String] = {
    valueColPrefixMap.map {case (valueCol, prefix) =>
      baseOutputColumnNames.map { outCol =>
        createColName(prefix, outCol)
      }
    }.flatten
  }

  /**
   * Create a derived column that contains the concatenation of all the values in
   * the pivot columns.  From the above columns, create a new column with following values:
   * | key |  _smv_pivot_val | count |
   * | --- | --------------- | ----- |
   * |  1  | Array(5_14_A)   |   100 |
   * |  1  | Array(5_14_B)   |   300 |
   * |  1  | Array(6_14_B)   |   200 |
   */
  private[smv] def addSmvPivotValColumn(origDF: DataFrame) : DataFrame = {
    import origDF.sqlContext.implicits._
    val normStr = udf({s:String => SchemaEntry.valueToColumnName(s)})
    val pivotColsExprSets = pivotColSets.map(a => a.map(s => normStr($"$s".cast(StringType))))

    val arrayExp = pivotColsExprSets.map{ pivotColsExpr =>
      concat_ws("_", pivotColsExpr: _*)
    }

    origDF.selectPlus(array(arrayExp: _*) as tempPivotValCol)
  }

  /**
   * Map the output column to the corresponding output column Given the output from addSmvPivotValColumn.
   * The expected output is:
   * | key | count_5_14_A | count_5_14_B | .... |
   * | --- | ------------ | ------------ | ---- |
   * |  1  |     100      |    NULL      | NULL |
   * |  1  |    NULL      |     300      | NULL |
   * |  1  |    NULL      |    NULL      | NULL |
   */
  private[smv] def mapValColsToOutputCols(srddWithPivotValCol: DataFrame, keyCols: Seq[Column]) = {
    import srddWithPivotValCol.sqlContext.implicits._
    srddWithPivotValCol.select(keyCols ++ outputColExprs: _*)
  }

}

private[smv] object SmvPivot {
  /**
   * Extract the column names from the data.
   * This is done by gettting the ditinct values of concated pivot columns.
   * Example:
   * | id  | month | product | count |
   * | --- | ----- | ------- | ----- |
   * | 1   | 5/14  |   A     |   100 |
   * | 1   | 6/14  |   B     |   200 |
   * | 1   | 5/14  |   B     |   300 |
   *
   * Return: Seq["5_14_A", "5_14_B", "6_14_B"]
   * **NOTE**: it's not the cartesian product of distinct values of all the columns.
   *           Since there is no "b/14, A" combination,  the result has no that column either.
   *
   * WARNING: This operation have to scan the entire RDD 1 or more times. For
   * most production system, the result of this operation should be static, in
   * that case the result should be coded in modules instead of calling this
   * operation every time.
   *
   * TODO: Add a java log warning message
   */
  private[smv] def getBaseOutputColumnNames(df: DataFrame, pivotColsSets: Seq[Seq[String]]): Seq[String] = {
    // create set of distinct values.
    // this is a seq of array strings where each array is distinct values for a column.
    pivotColsSets.map{ pivotCols =>
      val colNames = df.select(smvStrCat("_", pivotCols.map{s => df(s)}: _*)).
        distinct.collect.map{r => r(0).toString}

      // ensure result column name is made up of valid characters.
      colNames.map(c => SchemaEntry.valueToColumnName(c)).sorted
    }.flatten
  }
}
