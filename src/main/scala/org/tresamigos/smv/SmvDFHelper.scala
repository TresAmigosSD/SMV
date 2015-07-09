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
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.{Column, ColumnName}
import org.apache.spark.sql.GroupedData
import org.apache.spark.sql.functions._
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.analysis._
import org.apache.spark.sql.catalyst.plans.{JoinType, Inner}

class SmvDFHelper(df: DataFrame) {

  // TODO: add schema file path as well.
  def saveAsCsvWithSchema(dataPath: String, schemaWithMeta: SmvSchema = null)(implicit ca: CsvAttributes) {

    val schema = if (schemaWithMeta == null) {SmvSchema.fromSchemaRDD(df)} else {schemaWithMeta}

    //Adding the header to the saved file all the time even when ca.hasHeader is
    //False.
    val fieldNames = df.schema.fieldNames
    val headerStr = fieldNames.map(_.trim).map(fn => "\"" + fn + "\"").
      mkString(ca.delimiter.toString)

    val csvHeaderRDD = df.sqlContext.sparkContext.parallelize(Array(headerStr),1)
    val csvBodyRDD = df.map(schema.rowToCsvString(_))

    //As far as I know the union maintain the order. So the header will end up being the
    //first line in the saved file.

    // Since on Linux, when file stored on local file system, the partitions are not
    // guaranteed in order when read back in, we need to only store the body w/o the header
    // val csvRDD = csvHeaderRDD.union(csvBodyRDD)
    val csvRDD = csvBodyRDD

    schema.saveToFile(df.sqlContext.sparkContext, SmvSchema.dataPathToSchemaPath(dataPath))
    csvRDD.saveAsTextFile(dataPath)
  }

  /**
   * Dump the schema and data of given srdd to screen for debugging purposes.
   * TODO: add debug flag to turn on/off this method.  Hmm, I think adding a flag would encourage people to leave this in code :-)
   */
  def dumpSRDD = {
    println(SmvSchema.fromSchemaRDD(df))
    df.collect.foreach(println)
  }

  /**
   * checkNames: Require all the list of strings are real column names
   */
  private[smv] def checkNames(names: Seq[String]) = {
    require(names.toSet subsetOf df.columns.toSet)
  }

  /**
   * selects all the current columns in current SRDD plus the supplied expressions.
   */
  def selectPlus(exprs: Column*): DataFrame = {
    val all = df.columns.map{l=>df(l)}
    df.select( all ++ exprs : _* )
  }

  /**
   * Same as selectPlus but the new columns are prepended to result.
   */
  def selectPlusPrefix(exprs: Column*): DataFrame = {
    val all = df.columns.map{l=>df(l)}
    df.select( exprs ++ all : _* )
  }

  /**
   * Remove columns from current DF
   */
  def selectMinus(s: String, others: String*): DataFrame = {
    val names = s +: others
    checkNames(names)
    val all = df.columns diff names
    df.select(all.map{l=>df(l)} : _* )
  }

  def selectMinus(cols: Column*): DataFrame = {
    val names = cols.map(_.getName)
    selectMinus(names(0), names.tail: _*)
  }

  def selectMinus(s1: Symbol, sleft: Symbol*): DataFrame =
    selectMinus(s1.name, sleft.map{l=>l.name}: _*)

  def renameField(namePairs: (String, String)*): DataFrame = {
    val namePairsMap = namePairs.toMap
    val renamedFields = df.columns.map {
      fn => df(fn) as namePairsMap.getOrElse(fn, fn)
    }
    df.select(renamedFields: _*)
  }
  def renameField(n1: (Symbol, Symbol), nleft: (Symbol, Symbol)*): DataFrame =
    renameField((n1 +: nleft).map{case(l, r) => (l.name, r.name)}: _*)

  def prefixFieldNames(prefix: String) : DataFrame = {
    val renamedFields = df.columns.map {
      fn => df(fn) as (prefix + fn)
    }
    df.select(renamedFields: _*)
  }

  def postfixFieldNames(postfix: String) : DataFrame = {
    val renamedFields = df.columns.map {
      fn => df(fn) as (fn + postfix)
    }
    df.select(renamedFields: _*)
  }

  private[smv] def joinUniqFieldNames(otherPlan: DataFrame, on: Column, joinType: String = "inner") : DataFrame = {
    val namesL = df.columns.toSet
    val namesR = otherPlan.columns.toSet

    val dup = (namesL & namesR).toSeq
    val renamedFields = dup.map{l => l -> ("_" + l)}

    df.join(otherPlan.renameField(renamedFields: _*), on: Column, joinType)
  }

  def joinByKey(otherPlan: DataFrame, keys: Seq[String], joinType: String): DataFrame = {
    import df.sqlContext.implicits._

    val rightKeys = keys.map{k => "_" + k}
    val renamedFields = keys.zip(rightKeys).map{case (l,r) => (l -> r)}
    val newOther = otherPlan.renameField(renamedFields: _*)
    val joinOpt = keys.zip(rightKeys).map{case (l, r) => ($"$l" === $"$r")}.reduce(_ && _)

    df.joinUniqFieldNames(newOther, joinOpt, joinType).selectMinus(rightKeys(0), rightKeys.tail: _*)
  }

  def dedupByKey(k1:String, kleft: String*) : DataFrame = {
    import df.sqlContext.implicits._
    val keys = k1 +: kleft
    val selectExpressions = df.columns.map {
      fn => first(fn) as fn
    }
    df.groupBy(keys.map{k => $"$k"}: _*).agg(selectExpressions(0), selectExpressions.tail: _*)
  }
  def dedupByKey(cols: Column*): DataFrame = {
    val names = cols.map(_.getName)
    dedupByKey(names(0), names.tail: _*)
  }
  def dedupByKey(k1: Symbol, kleft: Symbol*): DataFrame =
    dedupByKey(k1.name, kleft.map{l=>l.name}: _*)

  /** adds a rank column to an srdd. */
  def smvRank(rankColumnName: String, startValue: Long = 0) = {
    val oldSchema = SmvSchema.fromSchemaRDD(df)
    val newSchema = oldSchema ++ new SmvSchema(Seq(LongSchemaEntry(rankColumnName)))

    val res: RDD[Row] = df.rdd.
      zipWithIndex().
      map{ case (row, idx) =>
        new GenericRow(Array[Any](row.toSeq ++ Seq(idx + startValue): _*)) }

    df.sqlContext.applySchemaToRowRDD(res, newSchema)
  }

  /**
   * smvPivot adds the pivoted columns without additional
   * aggregation. In other words N records in, N records out
   *
   * Please note that no keyCols need to be provided, since all original
   * columns will be kept
   *
   * Eg.
   *   srdd.smvPivot(Seq("month", "product"))("count")("5_14_A", "5_14_B", "6_14_A", "6_14_B")
   *
   * Input
   * | id  | month | product | count |
   * | --- | ----- | ------- | ----- |
   * | 1   | 5/14  |   A     |   100 |
   * | 1   | 6/14  |   B     |   200 |
   * | 1   | 5/14  |   B     |   300 |
   *
   * Output
   * | id  | month | product | count | count_5_14_A | count_5_14_B | count_6_14_A | count_6_14_B |
   * | --- | ----- | ------- | ----- | ------------ | ------------ | ------------ | ------------ |
   * | 1   | 5/14  |   A     |   100 | 100          | NULL         | NULL         | NULL         |
   * | 1   | 6/14  |   B     |   200 | NULL         | NULL         | NULL         | 200          |
   * | 1   | 5/14  |   B     |   300 | NULL         | 300          | NULL         | NULL         |
   *
   **/
  def smvPivot(pivotCols: Seq[String]*)(valueCols: String*)(baseOutput: String*): DataFrame = {
    // TODO: handle baseOutput == null with inferring using getBaseOutputColumnNames
    val pivot= SmvPivot(pivotCols, valueCols.map{v => (v, v)}, baseOutput)
    pivot.createSrdd(df, df.columns)
  }

  def smvUnpivot(valueCols: String*): DataFrame = {
    new UnpivotOp(df, valueCols).unpivot()
  }
  def smvUnpivot(valueCol: Symbol, others: Symbol*): DataFrame =
    smvUnpivot((valueCol +: others).map{s => s.name}: _*)

  /**
   * See RollupCubeOp and smvCube in SmvGroupedData.scala for details.
   *
   * Example:
   *   df.smvCube("zip", "month").agg("zip", "month", sum("v") as "v")
   *
   * Also have a version on SmvGroupedData.
   **/
  def smvCube(col: String, others: String*): SmvGroupedData = {
    new RollupCubeOp(df, Nil, (col +: others)).cube()
  }

  def smvCube(cols: Column*): SmvGroupedData = {
    val names = cols.map(_.getName)
    new RollupCubeOp(df, Nil, names).cube()
  }
  /**
   * See RollupCubeOp and smvCube in SmvGroupedData.scala for details.
   *
   * Example:
   *   df.smvRollup("county", "zip").agg("county", "zip", sum("v") as "v")
   *
   * Also have a version on SmvGroupedData
   **/
  def smvRollup(col: String, others: String*): SmvGroupedData = {
    new RollupCubeOp(df, Nil, (col +: others)).rollup()
  }

  def smvRollup(cols: Column*): SmvGroupedData = {
    val names = cols.map(_.getName)
    new RollupCubeOp(df, Nil, names).rollup()
  }
  /**
   * Create an Edd builder on DataFrame
   *
   * @param groupingExprs specify grouping expression(s) to compute Edd over
   * @return an Edd object
   */
  def groupEdd(groupingExprs : Column*): Edd = {
    Edd(df, groupingExprs)
  }

  /**
   * Create an Edd builder on DataFrame population
   */
  def edd: Edd = groupEdd()

  /**
   *  Similar to groupBy, instead of creating GroupedData,
   *  create an SmvGroupedData object
   */
  def smvGroupBy(cols: Column*) = {
    val names = cols.map{c => c.getName}
    SmvGroupedData(df, names)
  }

  def smvGroupBy(col: String, others: String*) = {
    SmvGroupedData(df, (col +: others))
  }

  /* Add back chunkByPlus for code migration */
  def chunkByPlus(keys: Symbol*)(chunkUDF: SmvChunkUDF) = {
    val kStr = keys.map{_.name}
    df.smvGroupBy(kStr(0), kStr.tail: _*).
      smvMapGroup(new SmvChunkUDFGDO(chunkUDF, true)).toDF
  }
}
