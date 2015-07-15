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
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Column, ColumnName}
import org.apache.spark.sql.functions._

/**
 * SmvPivot for Pivot Operations:
 *
 * Pivot Operation on SchemaRDD that transforms multiple rows per key into a single row for
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
 *        function, PivotOp.getBaseOutputColumnNames(schemaRDD, Seq(pivotCols))
 *        to generate baseOutputColumnNames. 
 */

case class SmvPivot(
        pivotColSets: Seq[Seq[String]],
        valueColPrefixMap: Seq[(String, String)],
        baseOutputColumnNames: Seq[String]) {
  require(!baseOutputColumnNames.isEmpty, "Pivot requires the output column names to be known")

  def createSrdd(srdd: SchemaRDD, keys: Seq[String]): SchemaRDD =
    mapValColsToOutputCols(addSmvPivotValColumn(srdd), keys.map{k => new ColumnName(k)})

  private val tempPivotValCol = "_smv_pivot_val"

  private val createColName = (prefix: String, baseOutCol: String) => prefix + "_" + baseOutCol
  private val contains: (Seq[Any], Any) => Boolean = { (a, v) => a.contains(v) }

  private val outputColExprs = valueColPrefixMap.map {case (valueCol, prefix) =>
    //  Zero filling is replaced by Null filling to handle CountDistinct right 
    baseOutputColumnNames.map { outCol =>
      new Column(SmvIfElseNull(
        ScalaUdf(contains, BooleanType, Seq((new ColumnName(tempPivotValCol)).toExpr, Literal(outCol))),
        (new ColumnName(valueCol)).toExpr
      )) as createColName(prefix, outCol)
    }
  }.flatten

  def outCols(): Seq[String] = {
    outputColExprs.map{l => l.toExpr.asInstanceOf[NamedExpression].name}
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
  private[smv] def addSmvPivotValColumn(origSRDD: SchemaRDD) : SchemaRDD = {
    import origSRDD.sqlContext.implicits._
    val pivotColsExprSets = pivotColSets.map(a => a.map(s => $"$s".toExpr))

    val arrayExp = pivotColsExprSets.map{ pivotColsExpr =>
      SmvPivotVal(pivotColsExpr)
    }

    origSRDD.selectPlus(new Column(SmvAsArray(arrayExp: _*)) as tempPivotValCol)
    //origSRDD.generate(Explode(Seq(tempPivotValCol.name), SmvAsArray(arrayExp: _*)), true)
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
  private[smv] def mapValColsToOutputCols(srddWithPivotValCol: SchemaRDD, keyCols: Seq[Column]) = {
    import srddWithPivotValCol.sqlContext.implicits._
    srddWithPivotValCol.select(keyCols ++ outputColExprs: _*)
  }

}

object SmvPivot {
  /**
   * Extract the column names from the data.
   * This is done by getting the distinct string values of each column and taking the cartesian product.
   * For N value columns, the output shall have N times as many columns as the returned list as
   * each item in the list would have the value column name(s) prepended.
   * Return: Seq["5_14_A", "5_14_B", ...]
   *
   * WARNING: This operation have to scan the entire RDD 1 or more times. For
   * most production system, the result of this operation should be static, in
   * that case the result should be coded in modules instead of calling this
   * operation every time.
   * 
   * TODO: Add a java log warning message
   */
  private[smv] def getBaseOutputColumnNames(schemaRDD: SchemaRDD, pivotColsSets: Seq[Seq[String]]): Seq[String] = {
    // create set of distinct values.
    // this is a seq of array strings where each array is distinct values for a column.
    pivotColsSets.map{ pivotCols =>
      val distinctVals = pivotCols.map{s => schemaRDD.select(s).
        distinct.collect.map { r =>
          Option(r(0)).getOrElse("").toString
        }}

      // get the cartesian product of all column values.
      val colNames = distinctVals.reduceLeft(
        (l1,l2) => for(v1 <- l1; v2 <- l2) yield (v1 + "_" + v2))

      // ensure result column name is made up of valid characters.
      colNames.map(c => SchemaEntry.valueToColumnName(c)).sorted
    }.flatten
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
