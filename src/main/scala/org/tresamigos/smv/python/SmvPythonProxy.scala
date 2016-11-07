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

package org.tresamigos.smv.python

import org.apache.spark._, sql._
import org.tresamigos.smv._
import py4j.GatewayServer

import scala.collection.JavaConversions._
import scala.util.Try
import java.util.ArrayList

/** Provides access to enhanced methods on DataFrame, Column, etc */
object SmvPythonHelper {
  def peekStr(df: DataFrame, pos: Int, colRegex: String): String = df._peek(pos, colRegex)

  def smvExpandStruct(df: DataFrame, cols: Array[String]): DataFrame =
    df.smvExpandStruct(cols: _*)

  def smvGroupBy(df: DataFrame, cols: Array[Column]): SmvGroupedDataAdaptor =
    new SmvGroupedDataAdaptor(df.smvGroupBy(cols:_*))

  def smvGroupBy(df: DataFrame, cols: Array[String]): SmvGroupedDataAdaptor = {
    import df.sqlContext.implicits._
    new SmvGroupedDataAdaptor(df.smvGroupBy(cols.toSeq.map{c => $"$c"}:_*))
  }

  /**
   * FIXME py4j method resolution with null argument can fail, so we
   * temporarily remove the trailing parameters till we can find a
   * workaround
   */
  def smvJoinByKey(df: DataFrame, other: DataFrame, keys: Seq[String], joinType: String): DataFrame =
    df.smvJoinByKey(other, keys, joinType)

  def smvJoinMultipleByKey(df: DataFrame, keys: Array[String], joinType: String): SmvMultiJoinAdaptor =
    new SmvMultiJoinAdaptor(df.smvJoinMultipleByKey(keys, joinType))

  def smvSelectMinus(df: DataFrame, cols: Array[String]): DataFrame =
    df.smvSelectMinus(cols.head, cols.tail:_*)

  def smvSelectMinus(df: DataFrame, cols: Array[Column]): DataFrame =
    df.smvSelectMinus(cols:_*)

  def smvDedupByKey(df: DataFrame, keys: Array[String]): DataFrame =
    df.dedupByKey(keys.head, keys.tail:_*)

  def smvDedupByKey(df: DataFrame, cols: Array[Column]): DataFrame =
    df.dedupByKey(cols:_*)

  def smvDedupByKeyWithOrder(df: DataFrame, keys: Array[String], orderCol: Array[Column]): DataFrame =
    df.dedupByKeyWithOrder(keys.head, keys.tail:_*)(orderCol:_*)

  def smvDedupByKeyWithOrder(df: DataFrame, cols: Array[Column], orderCol: Array[Column]): DataFrame =
    df.dedupByKeyWithOrder(cols:_*)(orderCol:_*)

  def smvRenameField(df: DataFrame, namePairsAsList: ArrayList[ArrayList[String]]): DataFrame = {
    val namePairs = namePairsAsList.map(inner => Tuple2(inner(0), inner(1)))
    df.renameField(namePairs:_*)
  }

  def smvConcatHist(df: DataFrame, colNames: Array[String]) = df._smvConcatHist(colNames.toSeq)

  def smvBinHist(df: DataFrame, _colWithBin: java.util.List[java.util.List[Any]]) = {
    val colWithBin = _colWithBin.map{t =>
      (t.get(0).asInstanceOf[String], t.get(1).asInstanceOf[Double])
    }.toSeq
    df._smvBinHist(colWithBin: _*)
  }

  def smvIsAllIn(col: Column, values: Any*): Column = col.smvIsAllIn(values:_*)
  def smvIsAnyIn(col: Column, values: Any*): Column = col.smvIsAnyIn(values:_*)

  //case class DiscoveredPK(pks: ArrayList[String], cnt: Long)
  def smvDiscoverPK(df: DataFrame, n: Int): (ArrayList[String], Long) = {
    val res = df.smvDiscoverPK(n, false)
    (new ArrayList(res._1), res._2)
  }

  /**
   * Update the port of callback client
   */
  def updatePythonGatewayPort(gws: GatewayServer, port: Int): Unit = {
    val cl = gws.getCallbackClient
    val f = cl.getClass.getDeclaredField("port")
    f.setAccessible(true)
    f.setInt(cl, port)
  }
}

class SmvGroupedDataAdaptor(grouped: SmvGroupedData) {
  def smvTopNRecs(maxElems: Int, orders: Array[Column]): DataFrame =
    grouped.smvTopNRecs(maxElems, orders:_*)

  def smvPivotSum(pivotCols: java.util.List[Array[String]],
    valueCols: Array[String], baseOutput: Array[String]): DataFrame =
    grouped.smvPivotSum(pivotCols.map(_.toSeq).toSeq :_*)(valueCols:_*)(baseOutput:_*)

  def smvFillNullWithPrevValue(orderCols: Array[Column], valueCols: Array[String]): DataFrame =
    grouped.smvFillNullWithPrevValue(orderCols: _*)(valueCols: _*)
}

class SmvMultiJoinAdaptor(joiner: SmvMultiJoin) {
  def joinWith(df: DataFrame, postfix: String, joinType: String): SmvMultiJoinAdaptor =
    new SmvMultiJoinAdaptor(joiner.joinWith(df, postfix, joinType))

  def doJoin(dropExtra: Boolean): DataFrame = joiner.doJoin(dropExtra)
}

/**
 * Provide app-level methods for use in Python.
 *
 * The collection types should be accessible through the py4j gateway.
 */
class SmvPythonApp(val app: SmvApp) {
  val config = app.smvConfig

  def callbackServerPort: Option[Int] = config.cmdLine.cbsPort.get

  def verifyConfig(): Unit = app.verifyConfig()

  /** The names of the modules to run in this app */
  def moduleNames(repo: SmvDataSetRepository): java.util.List[String] =
    app.modulesToRun(repo, app.scalaDataSets)

  /** Try to read a dataframe from persisted files */
  def tryReadPersistedFile(path: String): Try[DataFrame] =
    SmvUtil.readPersistedFile(app.sqlContext, path)

  /** Saves the dataframe to disk */
  def persist(dataframe: DataFrame, path: String, generateEdd: Boolean): Unit =
    SmvUtil.persist(app.sqlContext, dataframe, path, generateEdd)

  /** Export as hive table */
  def exportHive(dataframe: DataFrame, tableName: String): Unit =
    SmvUtil.exportHive(dataframe, tableName)

  /** Create a SmvCsvFile for use in Python */
  def smvCsvFile(moduleName: String, path: String, csvAttr: CsvAttributes,
    pForceParserCheck: Boolean, pFailAtParsingError: Boolean
  ): SmvCsvFile =
    new SmvCsvFile(path, csvAttr) {
      override def name = moduleName
      override val forceParserCheck = pForceParserCheck
      override val failAtParsingError = pFailAtParsingError
    }

  /** Output directory for files */
  def outputDir: String = app.smvConfig.outputDir

  /** Used to create small dataframes for testing */
  def dfFrom(schema: String, data: String): DataFrame =
    app.createDF(schema, data)

  /** Runs an SmvModule written in either Python or Scala */
  def runModule(modfqn: String, repo: SmvDataSetRepository): DataFrame =
    app.runModule(modfqn, repo, app.scalaDataSets)

  def runDynamicModule(modfqn: String, repo: SmvDataSetRepository): DataFrame =
    app.runDynamicModule(modfqn, repo, app.scalaDataSets)
}

/** Not a companion object because we need to access it from Python */
object SmvPythonAppFactory {
  def init(sqlContext: SQLContext): SmvPythonApp = init(Array("-m", "None"), sqlContext)

  def init(args: Array[String], sqlContext: SQLContext): SmvPythonApp =
    new SmvPythonApp(SmvApp.init(args, Option(sqlContext.sparkContext), Option(sqlContext)))
}
