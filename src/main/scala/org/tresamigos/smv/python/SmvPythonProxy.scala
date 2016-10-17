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
  // TODO relocate moduleNames() from SmvConfig to here
  val moduleNames: Array[String] = config.moduleNames

  /** The name to dataframe look-up table */
  var runParams: Map[String, DataFrame] = Map.empty

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
  def smvCsvFile(moduleName: String, path: String, csvAttr: CsvAttributes): SmvCsvFile =
    new SmvCsvFile(path, csvAttr) { override def name = moduleName }

  /** Output directory for files */
  def outputDir: String = app.smvConfig.outputDir

  /** Used to create small dataframes for testing */
  def dfFrom(schema: String, data: String): DataFrame =
    app.createDF(schema, data)

  /** Runs an SmvModule written in either Python or Scala */
  def runModule(modfqn: String, repo: SmvDataSetRepository): DataFrame = {
    // TODO: SmvApp can also implement SmvModuleRepository
    // first try to resolve module in the scala implementation; if not
    // found, use the callback to run
    ???
  }
}

/** Not a companion object because we need to access it from Python */
object SmvPythonAppFactory {
  def init(sqlContext: SQLContext): SmvPythonApp = init(Array("-m", "None"), sqlContext)

  def init(args: Array[String], sqlContext: SQLContext): SmvPythonApp =
    new SmvPythonApp(SmvApp.init(args, Option(sqlContext.sparkContext), Option(sqlContext)))
}
