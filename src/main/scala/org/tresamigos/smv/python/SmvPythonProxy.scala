package org.tresamigos.smv.python

import org.apache.spark._, sql._
import org.tresamigos.smv._

import scala.collection.JavaConversions._
import scala.util.Try

/** Provides access to enhanced methods on DataFrame, Column, etc */
object SmvPythonHelper {
  def peekStr(df: DataFrame, pos: Int, colRegex: String): String = df._peek(pos, colRegex)

  def smvExpandStruct(df: DataFrame, cols: Array[String]): DataFrame =
    df.smvExpandStruct(cols: _*)

  def smvGroupBy(df: DataFrame, cols: Array[Column]): SmvGroupedDataAdaptor =
    new SmvGroupedDataAdaptor(df.smvGroupBy(cols:_*))

  def smvJoinByKey(df: DataFrame, other: DataFrame, keys: Array[String], joinType: String): DataFrame =
    df.joinByKey(other, keys.toSeq, joinType)

  def smvHashSample(df: DataFrame, key: Column, rate: Double, seed: Int): DataFrame =
    df.smvHashSample(key, rate, seed)

  def smvSelectMinus(df: DataFrame, cols: Array[String]): DataFrame =
    df.smvSelectMinus(cols.head, cols.tail:_*)

  def smvSelectMinus(df: DataFrame, cols: Array[Column]): DataFrame =
    df.smvSelectMinus(cols:_*)

  def smvSelectPlus(df: DataFrame, cols: Array[Column]) = df.smvSelectPlus(cols:_*)

  def smvDedupByKey(df: DataFrame, keys: Array[String]): DataFrame =
    df.dedupByKey(keys.head, keys.tail:_*)

  def smvDedupByKey(df: DataFrame, cols: Array[Column]): DataFrame =
    df.dedupByKey(cols:_*)

  def smvDedupByKeyWithOrder(df: DataFrame, keys: Array[String], orderCol: Array[Column]): DataFrame =
    df.dedupByKeyWithOrder(keys.head, keys.tail:_*)(orderCol:_*)

  def smvDedupByKeyWithOrder(df: DataFrame, cols: Array[Column], orderCol: Array[Column]): DataFrame =
    df.dedupByKeyWithOrder(cols:_*)(orderCol:_*)

  def smvUnion(df: DataFrame, dfothers: Array[DataFrame]): DataFrame =
    df.smvUnion(dfothers:_*)

  def smvEdd(df: DataFrame, colNames: Array[String]) = df._smvEdd(colNames: _*)
  def smvHist(df: DataFrame, colNames: Array[String]) = df._smvHist(colNames: _*)
  def smvConcatHist(df: DataFrame, colNames: Array[String]) = df._smvConcatHist(colNames.toSeq)
  def smvFreqHist(df: DataFrame, colNames: Array[String]) = df._smvFreqHist(colNames: _*)
  def smvCountHist(df: DataFrame, keys: Array[String], binSize: Int) = df._smvCountHist(keys.toSeq, binSize)
  def smvBinHist(df: DataFrame, _colWithBin: java.util.List[java.util.List[Any]]) = {
    val colWithBin = _colWithBin.map{t =>
      (t.get(0).asInstanceOf[String], t.get(1).asInstanceOf[Double])
    }.toSeq
    df._smvBinHist(colWithBin: _*)
  }
}

class SmvGroupedDataAdaptor(grouped: SmvGroupedData) {
  def smvTopNRecs(maxElems: Int, orders: Array[Column]): DataFrame =
    grouped.smvTopNRecs(maxElems, orders:_*)

  def smvPivotSum(pivotCols: java.util.List[Array[String]],
    valueCols: Array[String], baseOutput: Array[String]): DataFrame =
    grouped.smvPivotSum(pivotCols.map(_.toSeq).toSeq :_*)(valueCols:_*)(baseOutput:_*)
}

/**
 * Provide app-level methods for use in Python.
 *
 * The collection types should be accessible through the py4j gateway.
 */
class SmvPythonApp(val app: SmvApp) {
  val config = app.smvConfig

  def verifyConfig(): Unit = app.verifyConfig()

  /** The names of the modules to run in this app */
  // TODO relocate moduleNames() from SmvConfig to here
  val moduleNames: Array[String] = app.smvConfig.moduleNames

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
}

/** Not a companion object because we need to access it from Python */
object SmvPythonAppFactory {
  def init(sqlContext: SQLContext): SmvPythonApp = init(Array("-m", "None"), sqlContext)

  def init(args: Array[String], sqlContext: SQLContext): SmvPythonApp =
    new SmvPythonApp(SmvApp.init(args, Option(sqlContext.sparkContext), Option(sqlContext)))
}
