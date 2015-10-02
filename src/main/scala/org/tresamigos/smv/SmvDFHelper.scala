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
import org.apache.spark.sql.{DataFrame, Column}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{StructField, StructType, LongType}
import org.apache.spark.sql.catalyst.expressions._

import org.apache.spark.annotation.Experimental
import cds._
import edd._

class SmvDFHelper(df: DataFrame) {

  /**
   * persist the `DataFrame` as a CSV file (along with a schema file).
   * {{{
   *   df.saveAsCsvWithSchema("/tmp/output/test.csv")
   * }}}
   *
   * @param dataPath direct path where file is persisted.  Can also be a relative path.  The configured app data/output dir are not considered.
   * @param ca CSV attributes used to format output file.  Defaults to `CsvAttributes.defaultCsv`
   * @param schemaWithMeta Provide the companion schema (usually used when we need to persist some schema meta data along with the standard schema)
   */
  def saveAsCsvWithSchema(dataPath: String, ca: CsvAttributes = CsvAttributes.defaultCsv, schemaWithMeta: SmvSchema = null) {
    val handler = new FileIOHandler(df.sqlContext, dataPath)
    handler.saveAsCsvWithSchema(df, schemaWithMeta, ca)
  }

  /**
   * Dump the schema and data of given df to screen for debugging purposes.
   * Similar to `show()` method of DF from Spark 1.3, although the format is slightly different.
   * This function's format is more convenient for us and hence has remained un-deprecated.
   */
  def dumpSRDD = {
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
   * Example 1:
   * {{{
   *   df.selectWithReplace($"age" + 1 as "age")
   * }}}
   *
   * will create a new data frame with the same schema and with all
   * values in the "age" column incremented by 1
   *
   * Example 2:
   * {{{
   * df.selectWithReplace($"age" + 1)
   * }}}
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
   * selects all the current columns in current `DataFrame` plus the supplied expressions.
   * The new columns are added to the end of the current column list.
   * {{{
   *   df.selectPlus($"price" * $"count" as "amt")
   * }}}
   */
  def selectPlus(exprs: Column*): DataFrame = {
    val all = df.columns.map{l=>df(l)}
    df.select( all ++ exprs : _* )
  }

  /**
   * Same as selectPlus but the new columns are prepended to result.
   * {{{
   *   df.selectPlusPrefix($"price" * $"count" as "amt")
   * }}}
   * `amt` will be the first column in the output.
   */
  def selectPlusPrefix(exprs: Column*): DataFrame = {
    val all = df.columns.map{l=>df(l)}
    df.select( exprs ++ all : _* )
  }

  /**
   * Remove one or more columns from current DataFrame.
   * Column names are specified as string.
   * {{{
   *   df.selectMinus("col1", "col2")
   * }}}
   */
  def selectMinus(s: String, others: String*): DataFrame = {
    val names = s +: others
    checkNames(names)
    val all = df.columns diff names
    df.select(all.map{l=>df(l)} : _* )
  }

  /**
   * Remove one or more columns from current DataFrame.
   * Column names are specified as `Column`
   * {{{
   *   df.selectMinus($"col1", df("col2"))
   * }}}
   */
  def selectMinus(cols: Column*): DataFrame = {
    val names = cols.map(_.getName)
    selectMinus(names(0), names.tail: _*)
  }

  /**
   * Remove one or more columns from current DataFrame.
   * Column names are specified as `Column`
   * {{{
   *   df.selectMinus('col1, 'col2)
   * }}}
   */
  @deprecated
  def selectMinus(s1: Symbol, sleft: Symbol*): DataFrame =
    selectMinus(s1.name, sleft.map{l=>l.name}: _*)

  /**
   * Rename one or more fields of a `DataFrame`.
   * The old/new names are given as string pairs.
   * {{{
   *   df.renameField( "a" -> "aa", "b" -> "bb" )
   * }}}
   */
  def renameField(namePairs: (String, String)*): DataFrame = {
    val namePairsMap = namePairs.toMap
    val renamedFields = df.columns.map {
      fn => df(fn) as namePairsMap.getOrElse(fn, fn)
    }
    df.select(renamedFields: _*)
  }

  /**
   * Rename one or more fields of a `DataFrame`.
   * The old/new names are given as symbol pairs.
   * {{{
   *   df.renameField( 'a -> 'aa, 'b -> 'bb )
   * }}}
   */
  @deprecated
  def renameField(n1: (Symbol, Symbol), nleft: (Symbol, Symbol)*): DataFrame =
    renameField((n1 +: nleft).map{case(l, r) => (l.name, r.name)}: _*)

  /**
   * Apply a prefix to all column names in the given `DataFrame`.
   * For Example:
   * {{{
   *   df.prefixFieldNames("x_")
   * }}}
   * The above will add "x_" to the beginning of every column name in the `DataFrame`
   */
  def prefixFieldNames(prefix: String) : DataFrame = {
    val renamedFields = df.columns.map {
      fn => df(fn) as (prefix + fn)
    }
    df.select(renamedFields: _*)
  }

  /**
   * Apply a posfix to all column names in the given `DataFrame`.
   * For Example:
   * {{{
   *   df.posfixFieldNames("_x")
   * }}}
   * The above will add "_x" to the end of every column name in the `DataFrame`
   */
  def postfixFieldNames(postfix: String) : DataFrame = {
    val renamedFields = df.columns.map {
      fn => df(fn) as (fn + postfix)
    }
    df.select(renamedFields: _*)
  }

  /**
   * Perform a join of the left/right `DataFrames` and rename duplicated column names by
   * prefixing them with "_" on the right hand side.
   *
   * '''Note:''' This will become private or removed in future SMV versions.
   * 
   * TODO: remove project dependency on this, and make it private[smv]
   */
  @Experimental
  def joinUniqFieldNames(otherPlan: DataFrame, on: Column, joinType: String = "inner") : DataFrame = {
    val namesL = df.columns.toSet
    val namesR = otherPlan.columns.toSet

    val dup = (namesL & namesR).toSeq
    val renamedFields = dup.map{l => l -> ("_" + l)}

    df.join(otherPlan.renameField(renamedFields: _*), on: Column, joinType)
  }

  /**
   * The Spark `DataFrame` join operation does not handle duplicate key neames.
   * If both left and right side of the join operation contain the same key, the result `DataFrame` is unusable.
   * The `joinByKey` method will allow the user to join two `DataFrames` using the same join key.
   * Post join, only the left side keys will remain.
   * {{{
   *   df1.joinByKey(df2, Seq("k"), SmvJoinType.Inner)
   * }}}
   * Note the use of the `SmvJoinType.Inner` const instead of the naked "inner" string.
   *
   * If, in addition to the duplicate keys, both df1 and df2 have column with name "v", both will be kept in the result,
   * but the df2 version will be prefix with "_".
   */
  def joinByKey(otherPlan: DataFrame, keys: Seq[String], joinType: String): DataFrame = {
    import df.sqlContext.implicits._

    val rightKeys = keys.map{k => "_" + k}
    val renamedFields = keys.zip(rightKeys).map{case (l,r) => (l -> r)}
    val newOther = otherPlan.renameField(renamedFields: _*)
    val joinOpt = keys.zip(rightKeys).map{case (l, r) => ($"$l" === $"$r")}.reduce(_ && _)

    df.joinUniqFieldNames(newOther, joinOpt, joinType).selectMinus(rightKeys(0), rightKeys.tail: _*)
  }

  /**
   * Remove duplicate records from the `DataFrame` by arbitrarly selecting the first record
   * from a set of records with same primary key or key combo.
   * For example, given the following input DataFrame:
   * {{{
   * | id  | product | Company |
   * | --- | ------- | ------- |
   * | 1   | A       | C1      |
   * | 1   | C       | C2      |
   * | 2   | B       | C3      |
   * | 2   | B       | C4      |
   * }}}
   *
   * and the following call:
   * {{{
   *   df.debupByKey("id")
   * }}}
   * will yield the following `DataFrame`:
   * {{{
   * | id  | product | Company |
   * | --- | ------- | ------- |
   * | 1   | A       | C1      |
   * | 2   | B       | C3      |
   * }}}
   *
   * while the following call:
   * {{{
   *   df.debupByKey("id", "product")
   * }}}
   *
   * will yield the following:
   * {{{
   * | id  | product | Company |
   * | --- | ------- | ------- |
   * | 1   | A       | C1      |
   * | 1   | C       | C2      |
   * | 2   | B       | C3      |
   * }}}
   */
  def dedupByKey(k1: String, krest: String*) : DataFrame = {
    import df.sqlContext.implicits._
    val keys = k1 +: krest
    val selectExpressions = df.columns.map {
      // TODO: use smvFirst instead of first, since `first` return the first non-null of each field
      fn => first(fn) as fn
    }
    df.groupBy(keys.map{k => $"$k"}: _*).agg(selectExpressions(0), selectExpressions.tail: _*)
  }

  /** Same as `dedupByKey(String*)` but uses `Column` to specify the key columns */
  def dedupByKey(cols: Column*): DataFrame = {
    val names = cols.map(_.getName)
    dedupByKey(names(0), names.tail: _*)
  }

  /** Same as `dedupByKey(String*)` but uses `Symbol` to specify the key columns */
  @deprecated
  def dedupByKey(k1: Symbol, kleft: Symbol*): DataFrame =
    dedupByKey(k1.name, kleft.map{l=>l.name}: _*)

  /**
   * Add a rank/sequence column to a DataFrame.
   * It uses `zipWithIndex` method of `RDD` to add a sequence number to records in a DF.
   * It ranks records sequentially by partition.
   * Please refer to Spark's document for the detail behavior of `zipWithIndex`.
   * '''Note:''' May force an action on the DataFrame if the DataFrame has more than one partition.
   *
   * {{{
   *   df.smvRank("seqId", 100L)
   * }}}
   * Create a new column named "seqId" and start from 100.
   */
  def smvRank(rankColumnName: String, startValue: Long = 0) = {
    val oldSchema = df.schema
    val newSchema = StructType(oldSchema.fields :+ StructField(rankColumnName, LongType, true))

    val res: RDD[Row] = df.rdd.
      zipWithIndex().
      map{ case (row, idx) =>
        new GenericRow(Array[Any](row.toSeq ++ Seq(idx + startValue): _*)) }

    df.sqlContext.createDataFrame(res, newSchema)
  }

  /**
   * smvPivot adds the pivoted columns without additional
   * aggregation. In other words N records in, N records out
   *
   * Please note that no keyCols need to be provided, since all original
   * columns will be kept
   *
   * Example:
   * {{{
   *   df.smvPivot(Seq("month", "product"))("count")("5_14_A", "5_14_B", "6_14_A", "6_14_B")
   * }}}
   *
   * {{{
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
   * }}}
   *
   * @param pivotCols The sequence of column names whose values will be used as the output pivot column names.
   * @param valueCols The columns whose value will be copied to the pivoted output columns.
   * @param baseOutput The expected base output column names (without the value column prefix).
   *                   The user is required to supply the list of expected pivot column output names to avoid
   *                   and extra action on the input DataFrame just to extract the possible pivot columns.
   */
  def smvPivot(pivotCols: Seq[String]*)(valueCols: String*)(baseOutput: String*): DataFrame = {
    // TODO: handle baseOutput == null with inferring using getBaseOutputColumnNames
    val pivot= SmvPivot(pivotCols, valueCols.map{v => (v, v)}, baseOutput)
    pivot.createSrdd(df, df.columns)
  }

  /**
   * Almost the opposite of the pivot operation.
   * Given a set of records with value columns, turns the value columns into value rows.
   * For example, Given the following input:
   * {{{
   * | id | X | Y | Z |
   * | -- | - | - | - |
   * | 1  | A | B | C |
   * | 2  | D | E | F |
   * | 3  | G | H | I |
   * }}}
   * and the following command:
   * {{{
   *   df.smvUnpivot("X", "Y", "Z")
   * }}}
   * will result in the following output:
   * {{{
   * | id | column | value |
   * | -- | ------ | ----- |
   * |  1 |   X    |   A   |
   * |  1 |   Y    |   B   |
   * |  1 |   Z    |   C   |
   * | ...   ...      ...  |
   * |  3 |   Y    |   H   |
   * |  3 |   Z    |   I   |
   * }}}
   *
   * '''Warning:''' This only works for String columns for now (due to limitation of Explode method)
   */
  def smvUnpivot(valueCols: String*): DataFrame = {
    new UnpivotOp(df, valueCols).unpivot()
  }

  /** same as `smvUnpivot(String*)` but uses `Symbol` to specify the value columns. */
  @deprecated
  def smvUnpivot(valueCol: Symbol, others: Symbol*): DataFrame =
    smvUnpivot((valueCol +: others).map{s => s.name}: _*)

  /**
   * See RollupCubeOp and smvCube in SmvGroupedData.scala for details.
   *
   * Example:
   * {{{
   *   df.smvCube("zip", "month").agg("zip", "month", sum("v") as "v")
   * }}}
   *
   * Also have a version on SmvGroupedData.
   **/
  @deprecated("should use spark cube method", "1.5")
  def smvCube(col: String, others: String*): SmvGroupedData = {
    new RollupCubeOp(df, Nil, (col +: others)).cube()
  }

  @deprecated("should use spark cube method", "1.5")
  def smvCube(cols: Column*): SmvGroupedData = {
    val names = cols.map(_.getName)
    new RollupCubeOp(df, Nil, names).cube()
  }

  /**
   * See RollupCubeOp and smvCube in SmvGroupedData.scala for details.
   *
   * Example:
   * {{{
   *   df.smvRollup("county", "zip").agg("county", "zip", sum("v") as "v")
   * }}}
   *
   * Also have a version on SmvGroupedData
   **/
  @deprecated("should use spark rollup method", "1.5")
  def smvRollup(col: String, others: String*): SmvGroupedData = {
    new RollupCubeOp(df, Nil, (col +: others)).rollup()
  }

  @deprecated("should use spark rollup method", "1.5")
  def smvRollup(cols: Column*): SmvGroupedData = {
    val names = cols.map(_.getName)
    new RollupCubeOp(df, Nil, names).rollup()
  }

  /**
   * Create an Edd on DataFrame.
   * See [[org.tresamigos.smv.edd.Edd]] for details.
   *
   * Example:
   * {{{
   * scala> df.summary().eddShow
   * }}}
   */
  @Experimental
  def edd: Edd = new Edd(df)

  /**
   * Similar to groupBy, instead of creating GroupedData, create an `SmvGroupedData` object.
   * See [[org.tresamigos.smv.SmvGroupedDataFunc]] for list of functions that can be applied to the grouped data.
   *
   * Note: This is going away shortly and user will be able to use standard Spark `groupBy` method directly.
   *
   * Example:
   * {{{
   *   df.smvGroup($"k").
   * }}}
   */
  @Experimental
  def smvGroupBy(cols: Column*) = {
    val names = cols.map{c => c.getName}
    SmvGroupedData(df, names)
  }

  /**
   * Same as `smvGroupBy(Column*)` but uses `String` to specify the columns.
   * Note: This is going away shortly and user will be able to use standard Spark `groupBy` method directly.
   */
  @Experimental
  def smvGroupBy(col: String, others: String*) = {
    SmvGroupedData(df, (col +: others))
  }

  /**
   * Apply user defined `chunk` mapping on data grouped by a set of keys
   *
   * {{{
   * val addFirst = (l: List[Seq[Any]]) => {
   *   val firstv = l.head.head
   *   l.map{r => r :+ firstv}
   * }
   * val addFirstFunc = SmvChunkUDF(
   *      Seq('time, 'call_length),
   *      SmvSchema.fromString("time: TimeStamp; call_length: Double; first_call_time: TimeStamp").toStructType,
   *      addFirst)
   * df.chunkBy('account, 'cycleId)(addFirstFunc)
   * }}}
   **/
  @deprecated("will rename and refine interface", "1.5")
  def chunkBy(keys: Symbol*)(chunkUDF: SmvChunkUDF) = {
    val kStr = keys.map{_.name}
    df.smvGroupBy(kStr(0), kStr.tail: _*).
      smvMapGroup(new SmvChunkUDFGDO(chunkUDF, false)).toDF
  }

  /**
   * Same as `chunkBy`, but add the new columns to existing columns
   **/
  @deprecated("will rename and refine interface", "1.5")
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
   *   df1.smvOverlapCheck("key")(df2, df3).edd.addHistogramTasks("flag")().dump
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
  @deprecated("should use spark df coalesce after 1.3", "1.5")
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
   * '''Warning''': Since using accumulator in process can't guarantee results when error recovery occcurs,
   * we will only use this method to report processed records when persisting SmvModule and potentially other SMV functions.
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
