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
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
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
  def saveAsCsvWithSchema(dataPath: String, ca: CsvAttributes = CsvAttributes.defaultCsv, schemaWithMeta: SmvSchema = null, strNullValue: String = "") {
    val handler = new FileIOHandler(df.sqlContext, dataPath)
    handler.saveAsCsvWithSchema(df, schemaWithMeta, ca, strNullValue)
  }

  /**
   * Dump the schema and data of given df to screen for debugging purposes.
   * Similar to `show()` method of DF from Spark 1.3, although the format is slightly different.
   * This function's format is more convenient for us and hence has remained un-deprecated.
   */
  def dumpSRDD = {
    val schema = SmvSchema.fromDataFrame(df)
    println(SmvSchema.fromDataFrame(df))
    df.collect.foreach(r => println(r.mkString(",")))
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
  @deprecated("use String instead of Symbol", "1.3")
  def selectMinus(s1: Symbol, sleft: Symbol*): DataFrame =
    selectMinus(s1.name, sleft.map{l=>l.name}: _*)

  /**
   * Rename one or more fields of a `DataFrame`.
   * The old/new names are given as string pairs.
   * {{{
   *   df.renameField( "a" -> "aa", "b" -> "bb" )
   * }}}
   *
   * The method preserves any pre-existing metadata associated with
   * renamed columns, whereas the method withColumnRenamed in Spark,
   * as of 1.5.2, would drop them.
   */
  def renameField(namePairs: (String, String)*): DataFrame = {
    val namePairsMap = namePairs.toMap

    // We don't want to rename to some field names which already exist
    val overlap = df.columns.intersect(namePairsMap.values.toSeq)
    if (!overlap.isEmpty) throw new IllegalArgumentException(
      "Rename to existing fields: " + overlap.mkString(", ")
    )

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
  @deprecated("use String instead of Symbol", "1.3")
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
   * Expand structure type column to a group of columns
   * Example input df:
   * {{{
   *  [id:string, address: struct<state:string, zip:string, street:string>]
   * }}}
   * output df:
   * {{{
   *  [id:string, state:string, zip:string, street:string]
   * }}}
   *
   * Example code:
   * {{{
   *  df.selectExpandStruct("address")
   * }}}
   **/
  def selectExpandStruct(colNames: String*): DataFrame = {
    checkNames(colNames)

    val subFields = colNames.map{n =>
      (n, df.schema.apply(n).dataType.asInstanceOf[StructType].fieldNames.toSeq)
    }.toMap

    val exprs = subFields.map{ case (col, fields) =>
      fields.map{f => df(col).getField(f) as f}
    }.flatten.toSeq

    df.selectPlus(exprs: _*).selectMinus(colNames.head, colNames.tail: _*)
  }

  /**
   * Perform a join of the left/right `DataFrames` and rename duplicated column names by
   * prefixing them with "_" on the right hand side if no `postfix` parameter specified,
   * otherwise postfixing the them.
   */
  private[smv] def joinUniqFieldNames(
    otherPlan: DataFrame,
    on: Column,
    joinType: String = "inner",
    postfix: String = null
  ) : DataFrame = {
    val namesLower = df.columns.map{c => c.toLowerCase}
    val renamedFields = otherPlan.columns.filter{c =>
      namesLower.contains(c.toLowerCase)
    }.map{c =>
      if(postfix == null) c -> ("_" + c)
      else c -> (c + postfix)
    }

    df.join(otherPlan.renameField(renamedFields: _*), on: Column, joinType)
  }

  /**
   * The Spark `DataFrame` join operation does not handle duplicate key names.
   * If both left and right side of the join operation contain the same key,
   * the result `DataFrame` is unusable.
   *
   * The `joinByKey` method will allow the user to join two `DataFrames` using the same join key.
   * Post join, only the left side keys will remain. In case of outer-join, the
   * `coalesce(leftkey, rightkey)` will replace the left key to be kept.
   *
   * {{{
   *   df1.joinByKey(df2, Seq("k"), SmvJoinType.Inner)
   * }}}
   * Note the use of the `SmvJoinType.Inner` const instead of the naked "inner" string.
   *
   * If, in addition to the duplicate keys, both df1 and df2 have column with name "v",
   * both will be kept in the result, but the df2 version will be prefix with "_" if no
   * `postfix` parameter is specified, otherwise df2 version with be postfixed with
   * the specified `postfix`.
   */
  def joinByKey(
    otherPlan: DataFrame,
    keys: Seq[String],
    joinType: String,
    postfix: String = null,
    dropRightKey: Boolean = true
  ): DataFrame = {
    import df.sqlContext.implicits._

    val rightKeys =
      if(postfix == null)
        keys.map{k => "_" + k}
      else
        keys.map{k => k + postfix}

    val joinedKeys = keys zip rightKeys
    val renamedFields = joinedKeys.map{case (l,r) => (l -> r)}
    val newOther = otherPlan.renameField(renamedFields: _*)
    val joinOpt = joinedKeys.map{case (l, r) => ($"$l" === $"$r")}.reduce(_ && _)

    val dfJoined = df.joinUniqFieldNames(newOther, joinOpt, joinType, postfix)
    val dfCoalescedKeys = joinType match {
      case SmvJoinType.Outer|SmvJoinType.RightOuter =>
        // for each key used in the outer-join, coalesce key value from left to right
        joinedKeys.foldLeft(dfJoined)((acc: DataFrame, keypair) => {
          val (lk, rk) = keypair
          acc.withColumn(lk, coalesce(acc(lk), acc(rk)))
        })
      case _ => dfJoined
    }
    dfCoalescedKeys.selectMinus(rightKeys(0), rightKeys.tail: _*)
  }

  /**
   * Create multiple DF join builder: `SmvMultiJoin`.
   *
   * Example:
   * {{{
   * df.joinMultipleByKey(Seq("k1", "k2"), Inner).
   *    joinWith(df2, "_df2").
   *    joinWith(df3, "_df3", LeftOuter).
   *    doJoin()
   * }}}
   *
   * In above example, `df` will inner join with `df2` on `k1` and `k2`, then
   * left outer join with `df3` with the same keys.
   * In the cases that there are columns with the same name, df2's column will be
   * renamed with postfix "_df2", and, df3's column will be renamed with postfix
   * "_df3".
   *
   * @param keys: Join key names
   * @param defaultJoinType: default join type
   *
   * @return an `SmvMultiJoin` object which support `joinWith` and `doJoin` method
   **/
  def joinMultipleByKey(keys: Seq[String], defaultJoinType: String) = {
    new SmvMultiJoin(Nil, SmvMultiJoinConf(df, keys, defaultJoinType))
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
    /* Should call dropDuplicates, but that method has bug as if the first record has null
    df.dropDuplicates(keys)*/

    val selectExpressions = df.columns.diff(keys).map {
      //using smvFirst instead of first, since `first` return the first non-null of each field
      fn => smvFirst($"$fn") as fn
    }

    if (selectExpressions.isEmpty) {
      df.select(k1, krest: _*).distinct()
    } else {
      val ordinals = df.schema.getIndices(keys: _*)
      val rowToKeys: Row => Seq[Any] = {row =>
        ordinals.map{i => row(i)}
      }

      val rdd = df.rdd.groupBy(rowToKeys).values.map{i => i.head}
      df.sqlContext.createDataFrame(rdd, df.schema)
      /* although above code pased all test cases. keep the original code here just in-case
      df.groupBy(keys.map{k => $"$k"}: _*).agg(selectExpressions(0), selectExpressions.tail: _*).
        select(df.columns.head, df.columns.tail: _*)
        */
    }
  }

  /** Same as `dedupByKey(String*)` but uses `Column` to specify the key columns */
  def dedupByKey(cols: Column*): DataFrame = {
    val names = cols.map(_.getName)
    dedupByKey(names(0), names.tail: _*)
  }

  /** Same as `dedupByKey(String*)` but uses `Symbol` to specify the key columns */
  @deprecated("use String instead of Symbol", "1.3")
  def dedupByKey(k1: Symbol, kleft: Symbol*): DataFrame =
    dedupByKey(k1.name, kleft.map{l=>l.name}: _*)

  /**
   * Remove duplicated records by selecting the first record regarding a given ordering
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
   *   df.debupByKeyWithOrder($"id")($"product".desc)
   * }}}
   * will yield the following `DataFrame`:
   * {{{
   * | id  | product | Company |
   * | --- | ------- | ------- |
   * | 1   | C       | C2      |
   * | 2   | B       | C3      |
   * }}}
   *
   * Same as the `dedupByKey` method, we use RDD groupBy in the implementation of this
   * method to make sure we can handel large key space.
   **/
  def dedupByKeyWithOrder(keyCol: Column*)(orderCol: Column*): DataFrame = {
    val keys = keyCol.map{c => c.getName}
    dedupByKeyWithOrder(keys.head, keys.tail: _*)(orderCol: _*)
  }

  /** Same as `dedupByKeyWithOrder(Column*)(Column*)` but use `String` as key **/
  def dedupByKeyWithOrder(k1: String, krest: String*)(orderCol: Column*): DataFrame = {
    val gdo = new cds.DedupWithOrderGDO(orderCol.map{o => o.toExpr}.toList)
    df.smvGroupBy(k1, krest: _*).
      smvMapGroup(gdo).toDF
  }

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
   * The reverse of smvPivot.  Specifically, given the following table
   *
   * +-------+-------+-------+-------+-------+-------+-------+-------+-------+-------+-------+-------+
   * |  Id   |  A_1  |  A_2  |  ...  | A_11  |  B_1  |  ...  | B_11  |  ...  |  Z_1  |  ...  | Z_11  |
   * +-------+-------+-------+-------+-------+-------+-------+-------+-------+-------+-------+-------+
   * |   1   | 1_a_1 | 1_a_2 |  ...  |1_a_11 | 1_b_1 |  ...  |1_b_11 |  ...  | 1_z_1 |  ...  |1_z_11 |
   * +-------+-------+-------+-------+-------+-------+-------+-------+-------+-------+-------+-------+
   * |   2   | 2_a_1 | 2_a_2 |  ...  |2_a_11 | 2_b_1 |  ...  |2_b_11 |  ...  | 2_z_1 |  ...  |2_z_11 |
   * +-------+-------+-------+-------+-------+-------+-------+-------+-------+-------+-------+-------+
   *
   * and a function that would map "A_1" to ("A", "1"), unpivoting all
   * columns except 'Id' (in other words, valueCols === columns - `Id`)
   * would transform the table into the following
   *
   * +-----+-----+------+------+-----+-------+
   * | Id  |Index|  A   |  B   | ... |  Z    |
   * +-----+-----+------+------+-----+-------+
   * |  1  |  1  |1_a_1 |1_b_1 | ... |1_z_1  |
   * +-----+-----+------+------+-----+-------+
   * |  1  |  2  |1_a_2 |1_b_2 | ... |1_z_2  |
   * +-----+-----+------+------+-----+-------+
   * | ... | ... | ...  | ...  | ... | ...   |
   * +-----+-----+------+------+-----+-------+
   * |  1  | 11  |1_a_11|1_b_11| ... |1_z_11 |
   * +-----+-----+------+------+-----+-------+
   * |  2  |  1  |2_a_1 |2_b_1 | ... | 2_z_1 |
   * +-----+-----+------+------+-----+-------+
   * |  2  |  2  |2_a_2 |2_b_2 | ... | 2_z_2 |
   * +-----+-----+------+------+-----+-------+
   * | ... | ... | ...  | ...  | ... |  ...  |
   * +-----+-----+------+------+-----+-------+
   * |  2  | 11  |2_a_11|2_b_11| ... |2_z_11 |
   * +-----+-----+------+------+-----+-------+
   *
   * See [[https://github.com/TresAmigosSD/SMV/issues/243 Issue 243]]
   *
   * @param valueCols    names of the columns to transpose
   * @param colNameFn    the function that takes a column name and returns a tuple2,
   *                     the first part is the transposed column name, the second part is the
   *                     value that goes into the Index column.
   * @param indexColName the name of the index column, if present, if None, no index column would be added
   */
  def smvUnpivot(valueCols: Seq[String],
    colNameFn: String => (String, String),
    indexColName: Option[String] = Some("Index")): DataFrame = {
    // see the inline comments in the returned tuple for this computation
    val (t1, t2, tbl) =
      valueCols.foldRight((Seq[String](), Seq[String](), Map[(String, String), String]())) {
        (vcol, acc) =>
        val (k, v) = colNameFn(vcol)
        (
          k +: acc._1, // unpivoted column name
          v +: acc._2, // unpivoted index value (row number)
          acc._3 + ((k, v) -> vcol) // the inverse of colNameFn to retrieve value in the dataframe cell
        )
      }

    val colNames = t1.distinct // collect the intended column names after unpivot
    // sort by length first, then in alphabetic order to simulate numeric ordering
    val indexValues = t2.distinct.sortWith((a,b) => a.length < b.length && a.compareTo(b) < 0)

    // need to make a name distinct from all the column names because
    // we are going to build a struct for each row, and the name is
    // used to retrieve the index value later
    val indexName = mkUniq(colNames, indexColName getOrElse "Index")

    val embedded = indexValues map { v =>
      val fields = lit(v).as(indexName) +: (for {
        k <- colNames
        col = tbl.get((k, v)).map(df(_)) getOrElse lit(null).cast(StringType)
      } yield col as k)

      struct(fields:_*)
    }

    val r1 = df.selectPlus(array(embedded:_*) as "_unpivoted_values")
    val r2 = r1.selectPlus(explode(r1("_unpivoted_values")) as "_kvpair")
    val r3 = indexColName match {
      case None => r2
      case Some(indexCol) => r2.selectPlus(r2("_kvpair")(indexName) as indexCol)
    }
    // now add each field in the embedded struct as a column
    r3.selectPlus((colNames.map(c => r3("_kvpair")(c) as c)):_*).
      selectMinus("_unpivoted_values", "_kvpair"). // remove intermediate results
      selectMinus(valueCols.head, valueCols.tail:_*)
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
  def smvUnpivot(valueCols: String*): DataFrame =
    smvUnpivot(valueCols, s => ("value", s), Some("column"))

  /** same as `smvUnpivot(String*)` but uses `Symbol` to specify the value columns. */
  @deprecated("use String instead of Symbol", "1.5")
  def smvUnpivot(valueCol: Symbol, others: Symbol*): DataFrame =
    smvUnpivot((valueCol +: others).map{s => s.name}: _*)

  /**
   * Similar to the `cube` Spark DF method, but using "*" instead of null to represent "Any"
   *
   * Example:
   * {{{
   *   df.smvCube("zip", "month").agg("zip", "month", sum("v") as "v")
   * }}}
   **/
  def smvCube(col: String, others: String*) = {
    new RollupCubeOp(df, Nil, (col +: others)).cube()
  }

  def smvCube(cols: Column*) = {
    val names = cols.map(_.getName)
    new RollupCubeOp(df, Nil, names).cube()
  }

  /**
   * Similar to the `rollup` Spark DF method, but using "*" instead of null to represent "Any"
   *
   * Example:
   * {{{
   *   df.smvRollup("county", "zip").agg("county", "zip", sum("v") as "v")
   * }}}
   **/
  def smvRollup(col: String, others: String*) = {
    new RollupCubeOp(df, Nil, (col +: others)).rollup()
  }

  def smvRollup(cols: Column*) = {
    new RollupCubeOp(df, Nil, cols.map(_.getName)).rollup()
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
   * Just an alias to smvGroupBy to make client code more readable
   **/
  def smvWithKeys(cols: String*) = {
    SmvDFWithKeys(df, cols)
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
   *
   * TODO: Current version will not keep teh key columns. It's SmvChunkUDF's responsibility to
   * make sure key column is carried. This behavior should be changed to automatically
   * carry keys, as chanegs made on Spark's groupBy.agg
   **/
  @deprecated("will rename and refine interface", "1.5")
  def chunkBy(keys: Symbol*)(chunkUDF: SmvChunkUDF) = {
    val kStr = keys.map{_.name}
    df.smvGroupBy(kStr(0), kStr.tail: _*).
      smvMapGroup(new SmvChunkUDFGDO(chunkUDF, false), false).toDF
  }

  /**
   * Same as `chunkBy`, but add the new columns to existing columns
   **/
  @deprecated("will rename and refine interface", "1.5")
  def chunkByPlus(keys: Symbol*)(chunkUDF: SmvChunkUDF) = {
    val kStr = keys.map{_.name}
    df.smvGroupBy(kStr(0), kStr.tail: _*).
      smvMapGroup(new SmvChunkUDFGDO(chunkUDF, true), false).toDF
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

  /**
   * Adds labels to the specified columns.
   *
   * Each column could have multiple labels.
   *
   * Example:
   * {{{
   *   val res = df.smvLabel("name", "sex")("red", "yellow").smvLabel("sex")("green")
   * }}}
   *
   * In this example, assume df has no labels, the res' "name" column will have
   * "red" and "yellow" labels, and "sex" column will have "red", "yellow", and "green"
   * labels.
   *
   * @param colNames: list of column names which the labels will be added, if empty all columns will be added the labels
   * @param labels: list of labels need to be added to the specified columns. Can't be empty
   */
  def smvLabel(colNames: String*)(labels: String*): DataFrame =
    (new SchemaMetaOps(df)).addLabel(colNames, labels)

  /** Returns all the labels on a specified column; throws if the column is missing */
  def smvGetLabels(col: String): Seq[String] =
    (new SchemaMetaOps(df)).getLabel(col)

  /**
   * Removes the specified labels from the specified columns.
   *
   * Example:
   * {{{
   *   df.smvRemoveLabel("sex")("yellow", "green")
   * }}}
   *
   * If no columns are specified, the specified labels are removed
   * from all applicable columns in the data frame.
   *
   * If no labels are specified, all labels are removed from the
   * specified columns.
   *
   * If neither columns nor labels are specified, i.e. both parameter
   * lists are empty, then all labels are removed from all columns in
   * the data frame, essentially clearing the label meta data.
   */
  def smvRemoveLabel(colNames: String*)(labels: String*): DataFrame =
    (new SchemaMetaOps(df)).removeLabel(colNames, labels)

  /**
   * Returns all column names in the data frame that contain all the
   * specified labels.  If the labels argument is an empty sequence,
   * returns all unlabeled columns in the data frame.
   *
   * Will throw if there are no columns that satisfy the condition.
   *
   * Example:
   * {{{
   *   val cols = df.smvWithLabel("A", "B")
   * }}}
   */
  def smvWithLabel(labels: String*): Seq[String] =
    (new SchemaMetaOps(df)).colWithLabel(labels)

  /**
   * DataFrame projection based on labels
   *
   * Example:
   * {{{
   *   val res = df.selectByLabel("yellow")
   * }}}
   **/
  def selectByLabel(labels: String*): DataFrame = {
    val cols = smvWithLabel(labels: _*).map{s => df(s)}
    df.select(cols: _*)
  }

  /**
   * Adds column descriptions
   *
   * Example:
   * {{{
   *   val res = df.smvDesc(
   *     "name" -> "This is customer's name",
   *     "sex"  -> "This is customer's self-identified sex"
   *   )
   * }}}
   **/
  def smvDesc(colDescs: (String, String)*): DataFrame =
    (new SchemaMetaOps(df)).addDesc(colDescs)

  /**
   * Return column description of a specified column (by name string)
   **/
  def smvGetDesc(colName: String): String =
    (new SchemaMetaOps(df)).getDesc(colName)

  /**
   * Return the sequence of field name - description pairs
   **/
  def smvGetDesc(): Seq[(String, String)] =
    df.columns.map{c => (c, smvGetDesc(c))}

  /**
   * Remove descriptions from specified columns (by name string)
   * If parameter is empty, {{{df.smvRemoveDesc()}}}, remove all descriptions
   **/
  def smvRemoveDesc(colNames: String*): DataFrame =
    (new SchemaMetaOps(df)).removeDesc(colNames)

  /**
   * Print column names with description
   * e.g.
   * {{{
   * scala> val res = df.smvDesc("a" -> "column a is ....")
   * scala> res.printDesc
   * }}}
   **/
  def printDesc() = {
    val discs = df.smvGetDesc().toMap
    val width = discs.keys.map{_.size}.max
    discs.foreach{case (n, d) => printf(s"%-${width}s: %s\n", n,d)}
  }

  /**
   * Display a dataframe row in transposed view.
   */
  def peek(pos: Int, colRegex: String = ".*"): Unit = {
    val rows = df.take(pos)

    if (!rows.isEmpty) {
      val r = df.take(pos).last

      val labels = for {
        (f, i) <- df.schema.zipWithIndex
        if colRegex.r.findFirstIn(f.name).isDefined
      } yield (s"${f.name}:${f.dataType.toString.replaceAll("Type", "")}", i)

      val width = labels.maxBy(_._1.length)._1.length
      labels.foreach { t =>
        printf(s"%-${width}s = %s\n", t._1, r(t._2))
      }
    } else {
      printf("Cannot peek an empty DataFrame")
    }
  }

  /**
   * Use default peek with or without the parenthesis
   **/
  def peek(): Unit = peek(1)

  def peek(colRegex: String): Unit = peek(1, colRegex)


  /**
   * Find column combinations which uniquely identify a row from the data
   *
   * @param n number of rows the PK discovery algorithm will run on.
   * @param debug if true printout debug info
   * @return (list_of_keys, unique-count)
   *
   * Please note the algorithm only look for a set of keys which uniquely
   * identify the row, there could be more key combinations which can also
   * be the primary key.
   */
  def smvDiscoverPK(n: Integer = 10000, debug: Boolean = false): (Seq[String], Long) = {
    val discoverer = new PrimaryKeyDiscovery(debug)
    discoverer.discoverPK(df, n)
  }

  /**
   * Export DF to local file system. Path is relative to the app runing dir
   *
   * @param path relative path to the app runing dir on local file system (instead of HDFS)
   * @param n number of records to be exported. Defualt is to export every records
   *
   * **NOTE** since we have to collect the DF and then call JAVA file operations, the job
   * have to be launched as either local or yar-client mode. Also it is user's responsibility
   * to make sure that the DF is small enought to fit into memory.
   **/
  def exportCsv(path: String, n: Integer = null) {
    val schema = SmvSchema.fromDataFrame(df)
    val ca = CsvAttributes.defaultCsv

    val schemaPath = SmvSchema.dataPathToSchemaPath(path)
    schema.saveToLocalFile(schemaPath)

    val qc = ca.quotechar
    val headerStr = df.columns.map(_.trim).map(fn => qc + fn + qc).
      mkString(ca.delimiter.toString)

    // issue #312: Spark's collect from a large partition is observed
    // to add duplicate records, hence we use coalesce to reduce the
    // number of partitions before calling collect
    val bodyStr = if(n == null) {
      df.map(schema.rowToCsvString(_, ca)).coalesce(4).collect.mkString("\n")
    } else {
      df.limit(n).map(schema.rowToCsvString(_, ca)).coalesce(4).collect.mkString("\n")
    }

    SmvReportIO.saveLocalReport(headerStr + "\n" + bodyStr + "\n", path)
  }

  /**
   * Add a set of DoubleBinHistogram columns to a DataFrame.
   * Perform a DoubleBinHistogram on all the columns_to_bin. The num_of_bins is the corresponding
   * number of bin for each column in columns_to_bin.
   * The default number of bin is 1000, if the size of num_of_bins is less then the size of columns_to_bin,
   * only the extra columns that does not have the corresponding number of bin will be default to 1000
   * The columns_to_bin are expected to be of type double
   *
   * {{{
   *   df.smvDoubleBinHistogram(Seq("key1", "key2"), Seq(col1, col2), Seq(100, 200))
   * }}}
   * Create a new columns named the same as the columns to bin post fixed with post_fix.
   * The post_fix  is defaulted to "_bin"
   */
  def smvDoubleBinHistogram(keys: Seq[String],
                            columns_to_bin: Seq[String],
                            num_of_bins: Seq[Int] = Seq[Int](),
                            post_fix: String = "_bin"): DataFrame = {

    if (columns_to_bin.isEmpty) {
      df
    } else {
      import df.sqlContext.implicits._

      val min_cols: Seq[Column] = columns_to_bin.map( col => min(col) as "_min_" + col )
      val max_cols: Seq[Column] = columns_to_bin.map( col => max(col) as "_max_" + col)
      val key_cols: Seq[Column] = keys.map( key => $"$key")

      val min_max_cols = min_cols ++ max_cols

      val min_max_df = df.groupBy(key_cols: _*).agg(min_max_cols(0), min_max_cols.tail: _*)


      val df_with_min_max = df.joinByKey(min_max_df, keys, SmvJoinType.Inner)

      var number_of_bins = num_of_bins
      //Make sure that size of number_of_bins is equal to size of columns_to_bin.
      //If not add default bin number which is 1000
      while(number_of_bins.length < columns_to_bin.length) {
        number_of_bins = number_of_bins :+ 1000
      }

      val min_col_names = columns_to_bin.map(col => "_min_" + col)
      val max_col_names = columns_to_bin.map(col => "_max_" + col)

      val bin_col_names = columns_to_bin.map( col => col + post_fix)

      val num_of_cols = columns_to_bin.length

      //Construct a list of tuples where each tuple holds info about a given col to bin.
      val cols_info = for (i <- 0 until num_of_cols)
            yield (columns_to_bin(i),  min_col_names(i),  max_col_names(i), number_of_bins(i), bin_col_names(i))

      //Construct the bining expressions
      val bin_cols_expr: Seq[Column] = for ((c_name, c_min, c_max, c_num_bin, c_bin_name) <- cols_info)
            yield DoubleBinHistogram($"$c_name", $"$c_min", $"$c_max", lit(c_num_bin)) as c_bin_name

      df_with_min_max.
        groupBy(key_cols: _*).
        agg(bin_cols_expr(0), bin_cols_expr.tail: _*)
    }
  }

  /**
   * Add a DoubleBinHistogram column to a DataFrame using multiple keys.
   * Perform a DoubleBinHistogram on  the column_to_bin using the passed number of bins num_of_bins
   * The column_to_bin is expected to be of type double
   *
   * {{{
   *   df.smvDoubleBinHistogram(Seq("key1", "key2"), col, 100)
   * }}}
   * Create a new column named the same as passed column name to bin post fixed with post_fix.
   */
  def smvDoubleBinHistogram(keys: Seq[String], column_to_bin: String, num_of_bins: Int, post_fix: String): DataFrame = {
    smvDoubleBinHistogram(keys, Seq(column_to_bin), Seq(num_of_bins), post_fix)
  }

  /**
   * Add a DoubleBinHistogram column to a DataFrame using multiple keys.
   * Perform a DoubleBinHistogram on  the column_to_bin using the passed number of bins num_of_bins
   * The column_to_bin is expected to be of type double
   *
   * {{{
   *   df.smvDoubleBinHistogram(Seq("key1", "key2"), col, 100)
   * }}}
   * Create a new column named the same as passed column name to bin post fixed with "_bin"
   */
  def smvDoubleBinHistogram(keys: Seq[String], column_to_bin: String, num_of_bins: Int): DataFrame = {
    smvDoubleBinHistogram(keys, Seq(column_to_bin), Seq(num_of_bins))
  }

  /**
   * Add a DoubleBinHistogram column to a DataFrame using multiple keys.
   * Perform a DoubleBinHistogram on  the column_to_bin using 1000 bins
   * The column_to_bin is expected to be of type double
   *
   * {{{
   *   df.smvDoubleBinHistogram(Seq("key1", "key2"), col)
   * }}}
   * Create a new column named the same as passed column name to bin post fixed with post_fix.
   */
  def smvDoubleBinHistogram(keys: Seq[String], column_to_bin: String, post_fix: String): DataFrame = {
    smvDoubleBinHistogram(keys, Seq(column_to_bin), Seq[Int](), post_fix)
  }

  /**
   * Add a DoubleBinHistogram column to a DataFrame using multiple keys.
   * Perform a DoubleBinHistogram on  the column_to_bin using 1000 bins
   * The column_to_bin is expected to be of type double
   *
   * {{{
   *   df.smvDoubleBinHistogram(Seq("key1", "key2"), col)
   * }}}
   * Create a new column named the same as passed column name to bin post fixed with "_bin"
   */
  def smvDoubleBinHistogram(keys: Seq[String], column_to_bin: String): DataFrame = {
    smvDoubleBinHistogram(keys, Seq(column_to_bin))
  }

  /**
   * Add a DoubleBinHistogram column to a DataFrame using single key.
   * Perform a DoubleBinHistogram on the column_to_bin using the passed number of bins num_of_bins
   * The column_to_bin is expected to be of type double
   *
   * {{{
   *   df.smvDoubleBinHistogram(key, col, 100, "_xyz")
   * }}}
   * Create a new column named the same as passed column name to bin post fixed with post_fix.
   */
  def smvDoubleBinHistogram(key: String, column_to_bin: String, num_of_bins: Int, post_fix: String): DataFrame = {
    smvDoubleBinHistogram(Seq(key), Seq(column_to_bin), Seq(num_of_bins), post_fix)
  }

  /**
   * Add a DoubleBinHistogram column to a DataFrame using single key.
   * Perform a DoubleBinHistogram on the column_to_bin using the passed number of bins num_of_bins
   * The column_to_bin is expected to be of type double
   *
   * {{{
   *   df.smvDoubleBinHistogram(key, col, 100)
   * }}}
   * Create a new column named the same as passed column name to bin post fixed with "_bin"
   */
  def smvDoubleBinHistogram(key: String, column_to_bin: String, num_of_bins: Int): DataFrame = {
    smvDoubleBinHistogram(Seq(key), Seq(column_to_bin), Seq(num_of_bins))
  }

  /**
   * Add a DoubleBinHistogram column to a DataFrame using single key.
   * Perform a DoubleBinHistogram on the column_to_bin using 1000 bins
   * The column_to_bin is expected to be of type double
   *
   * {{{
   *   df.smvDoubleBinHistogram(key1, col, "_xyz")
   * }}}
   * Create a new column named the same as passed column name to bin post fixed with post_fix.
   */
  def smvDoubleBinHistogram(key: String, column_to_bin: String, post_fix: String): DataFrame = {
    smvDoubleBinHistogram(Seq(key), Seq(column_to_bin), Seq[Int](), post_fix)
  }

  /**
   * Add a DoubleBinHistogram column to a DataFrame using single key.
   * Perform a DoubleBinHistogram on the column_to_bin using 1000 bins
   * The column_to_bin is expected to be of type double
   *
   * {{{
   *   df.smvDoubleBinHistogram(key1, col)
   * }}}
   * Create a new column named the same as passed column name to bin post fixed with "_bin"
   */
  def smvDoubleBinHistogram(key: String, column_to_bin: String): DataFrame = {
    smvDoubleBinHistogram(Seq(key), Seq(column_to_bin))
  }
}
