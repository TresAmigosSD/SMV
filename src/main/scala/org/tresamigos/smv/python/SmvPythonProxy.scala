package org.tresamigos.smv.python

import org.apache.spark._, sql._
import org.tresamigos.smv._

import scala.collection.JavaConversions._
import scala.util.Try

/** Provides access to enhanced methods on DataFrame, Column, etc */
object SmvPythonHelper {
  def peek(df: DataFrame) = df.peek()
  def selectPlus(df: DataFrame, cols: Array[Column]) = df.selectPlus(cols:_*)

  def smvGroupBy(df: DataFrame, cols: Array[Column]) =
    new SmvGroupedDataAdaptor(df.smvGroupBy(cols:_*))

  def smvJoinByKey(df: DataFrame, other: DataFrame, keys: Array[String], joinType: String) =
    df.joinByKey(other, keys.toSeq, joinType)
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

  /** Create a SmvCsvFile for use in Python */
  def smvCsvFile(moduleName: String, path: String, csv: String): SmvCsvFile = {
    val attr: CsvAttributes = csv match {
      case "schema" => null
      case "csv+h"  => CsvAttributes.defaultCsvWithHeader
      // TODO: finish all csv types
      case _        => throw new RuntimeException(s"csv format ${csv} is not supported")
    }

    new SmvCsvFile(path, attr) { override def name = moduleName }
  }

  /** Output directory for files */
  def outputDir: String = app.smvConfig.outputDir
}

/** Not a companion object because we need to access it from Python */
object SmvPythonAppFactory {
  def init(sqlContext: SQLContext): SmvPythonApp = init(Array("-m", "None"), sqlContext)

  def init(args: Array[String], sqlContext: SQLContext): SmvPythonApp =
    new SmvPythonApp(SmvApp.init(args, Option(sqlContext.sparkContext), Option(sqlContext)))
}
