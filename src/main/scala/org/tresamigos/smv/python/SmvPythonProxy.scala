package org.tresamigos.smv.python

import org.apache.spark._, sql._
import org.tresamigos.smv._

/** Use a class instead of object to avoid name-mangling when using py4j */
class SmvPythonAppFactory {
  def init(sqlContext: SQLContext): SmvPythonApp = init(Array("-m", "None"), sqlContext)

  def init(args: Array[String], sqlContext: SQLContext): SmvPythonApp =
    new SmvPythonApp(SmvApp.init(args, Option(sqlContext.sparkContext), Option(sqlContext)))

  def peek(df: DataFrame) = df.peek()
  def selectPlus(df: DataFrame, cols: Array[Column]) = df.selectPlus(cols:_*)
}

/**
 * Provide methods for use in Python.
 *
 * The collection types should be accessible through the py4j gateway.
 */
class SmvPythonApp(val app: SmvApp) {
  // TODO relocate moduleNames() from SmvConfig to here
  val moduleNames: Array[String] = app.smvConfig.moduleNames

  var runParams: Map[String, DataFrame] = Map.empty

  def readPersistedFile(path: String): DataFrame =
    SmvUtil.readPersistedFile(app.sqlContext, path).get

  def persist(dataframe: DataFrame, path: String, generateEdd: Boolean) =
    SmvUtil.persist(app.sqlContext, dataframe, path, generateEdd)

}
