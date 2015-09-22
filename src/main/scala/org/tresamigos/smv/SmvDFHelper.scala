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
import org.apache.spark.Accumulator
import org.apache.spark.sql.{DataFrame, GroupedData, Column, ColumnName}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.analysis._
import org.apache.spark.sql.catalyst.plans.{JoinType, Inner}

class SmvDFHelper(df: DataFrame) {

  // TODO: add schema file path as well.
  def saveAsCsvWithSchema(dataPath: String, schemaWithMeta: SmvSchema = null)(implicit ca: CsvAttributes) {
    val handler = new FileIOHandler(df.sqlContext)
    handler.saveAsCsvWithSchema(df, dataPath, schemaWithMeta, ca)
  }

  /**
   * Dump the schema and data of given df to screen for debugging purposes.
   */
  def dumpSRDD = {
    // TODO: use printSchema on df.
    println(SmvSchema.fromDataFrame(df))
    df.collect.foreach(println)
  }

  /**
   * checkNames: Require all the list of strings are real column names
   */
  private[smv] def checkNames(names: Seq[String]) = {
    require(names.toSet subsetOf df.columns.toSet)
  }

  /**
   * Add, or replace, columns to the data frame.
   *
   * Each column expression in the argument list is added to the data
   * frame.  If the column is an alias (NamedExpression), any existing
   * column by the same name as the alias will be replaced by the new
   * column data.
   *
   * Example:
   *
   * 1.  <code>
   *   df.selectWithReplace($"age" + 1 as "age")
   * </code>
   *
   * will create a new data frame with the same schema and with all
   * values in the "age" column incremented by 1
   *
   * 2. <code>
   *   df.selectWithReplace($"age" + 1)
   * </code>
   *
   * will create a new data frame with an additional column (named
   * automatically by spark sql) containing the incremented values in
   * the "age" column, unless there is already another column that
   * happens to have the same spark-generated name (in which case that
   * column will be replaced with the new expression)
   */
  def selectWithReplace(columns: Column*): DataFrame = {
    val currColNames: Seq[String] = df.columns

    // separate columns into an overwrite set and the rest, which will be simply added
    val (overwrite, add) = columns.partition(c => c.toExpr match {
      case alias: NamedExpression => currColNames.contains(alias.name)
      case _ => false
    })

    // Update the overwritten columns first, working with a smaller
    // set of total columns, then add the rest
    val edited = if (overwrite.isEmpty) df else {
      val origColNames: Seq[String] = overwrite.map(_.getName)
      val uniquelyNamed: Seq[Column] =
        overwrite.map(c => c as "_SelectWithReplace_" + c.getName) // expecting this to be good-enough to ensure column name uniqueness in the schema
      val renameArgs: Seq[(String, String)] = uniquelyNamed.map(_.getName) zip origColNames

      // add the new columns first, because they could (and usually)
      // refer to the columns being updated
      // the last select is to make sure the ordering of columns don't change
      df.selectPlus(uniquelyNamed:_*).
        selectMinus(origColNames.head, origColNames.tail:_*).
        renameField(renameArgs:_*).
        select(currColNames.head, currColNames.tail: _*)
    }

    edited.selectPlus(add: _*)
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

  def joinUniqFieldNames(otherPlan: DataFrame, on: Column, joinType: String = "inner") : DataFrame = {
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

  /** adds a rank column to an df. */
  def smvRank(rankColumnName: String, startValue: Long = 0) = {
    val oldSchema = SmvSchema.fromDataFrame(df)
    val newSchema = oldSchema ++ new SmvSchema(Seq(LongSchemaEntry(rankColumnName)))

    val res: RDD[Row] = df.rdd.
      zipWithIndex().
      map{ case (row, idx) =>
        new GenericRow(Array[Any](row.toSeq ++ Seq(idx + startValue): _*)) }

    df.sqlContext.createDataFrame(res, newSchema.toStructType)
  }

  /**
   * smvPivot adds the pivoted columns without additional
   * aggregation. In other words N records in, N records out
   *
   * Please note that no keyCols need to be provided, since all original
   * columns will be kept
   *
   * Eg.
   *   df.smvPivot(Seq("month", "product"))("count")("5_14_A", "5_14_B", "6_14_A", "6_14_B")
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

  /**
   * For a set of DFs, which share the same key column, check the overlap across them.
   *
   * {{{
   *   df1.smvOverlapCheck("key")(df2, df3, df4)
   * }}}
   *
   * The output is another DF with 2 columns:
   * {{{
   *    key, flag
   * }}}
   * where flag is a bit string, e.g. 0110. Each bit represent whether the original DF has
   * this key.
   *
   * It can be used with EDD to summarize on the flag:
   *
   * {{{
   *   df1.smvOverlapCheck("key")(df2, df3).edd.addHistogramTasks("flag")().Dump
   * }}}
   **/
  def smvOverlapCheck(key: String, partition: Int = 4)(dfother: DataFrame*) = {
    import df.sqlContext.implicits._

    val dfSimple = df.select($"${key}", $"${key}" as s"${key}_0").repartition(partition)
    val otherSimple = dfother.zipWithIndex.map{case (df, i) =>
      val newkey = s"${key}_${i+1}"
      (newkey, df.select($"${key}" as newkey).repartition(partition))
    }

    val joined = otherSimple.foldLeft(dfSimple){(c, p) =>
      val newkey = p._1
      val r = p._2
      c.join(r, $"${key}" === $"${newkey}", SmvJoinType.Outer).
        selectPlus(coalesce($"${key}", $"${newkey}") as "tmp").
        selectMinus(key).renameField("tmp" -> key)
    }

    val hasCols = Range(0, otherSimple.size + 1).map{i =>
      val newkey = s"${key}_${i}"
      columnIf($"${newkey}".isNull, "0", "1")
    }

    joined.select($"${key}", smvStrCat(hasCols: _*) as "flag")
  }

  /**
   * Sample the df according to the hash of a column.
   * MurmurHash3 algorithm is used for generating the hash
   *
   * {{{
   *  df.smvHashSample($"key", rate=0.1, seed=123)
   * }}}
   *
   * @param key column to sample on.
   * @param rate sample rate in range (0, 1] with a default of 0.01 (1%)
   * @param seed random generator integer seed with a default of 23.
   **/

  def smvHashSample(key: Column, rate: Double = 0.01, seed: Int = 23) = {
    import scala.util.hashing.{MurmurHash3=>MH3}
    val cutoff = Int.MaxValue * rate
    val getHash = {s: Any => MH3.stringHash(s.toString, seed) & Int.MaxValue}
    val hashUdf = udf(getHash)
    df.where(hashUdf(key) < lit(cutoff))
  }

  /**
   * DF level coalesce, for Spark 1.3 only. Should be removed and use DF method coalesce in 1.4
   **/
  def smvCoalesce(n: Int) = {
    df.sqlContext.createDataFrame(df.rdd.coalesce(n).map{r => Row.fromSeq(r.toSeq)}, df.schema)
  }

  /**
   * Increment accumulated count for each processed record in a data frame "in-flight".
   * This method will inject a udf to increment the given counter by one for each processed records.
   * The count is computed "in-flight" so that we do not need to force an action on the DataFrame.
   *
   * Example:
   * {{{
   *   val c = sc.accumulator(0l)
   *   val s1 = srdd.smvPipeCount(c)
   *   ....
   *   s1.saveAsCsvWithSchema("file")
   *   println(c.value)
   * }}}
   *
   * '''Warning''': Since using accumulator in process can't guarantee results when recover from
   * failures, we will only use this method to report processed records when persist SmvModule
   * and potentially other SMV functions.
   */
  private[smv] def smvPipeCount(counter: Accumulator[Long]): DataFrame = {
    counter.setValue(0l)
    val dummyFunc = udf({() =>
      counter += 1l
      true
    })

    df.where(dummyFunc())
  }
}
