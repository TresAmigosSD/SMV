package org.tresamigos.smv.python

import org.apache.spark._, sql._
import org.tresamigos.smv._

/** Use a class instead of object to avoid name-mangling when using py4j */
class SmvPythonAppFactory {
  def init(sqlContext: SQLContext): SmvPythonApp = init(Array("-m", "None"), sqlContext)

  def init(args: Array[String], sqlContext: SQLContext): SmvPythonApp =
    new SmvPythonApp(SmvApp.init(args, Option(sqlContext.sparkContext), Option(sqlContext)))
}

/** Provides access to enhanced methods on DataFrame, Column, etc */
class SmvPythonProxy {
  def peek(df: DataFrame) = df.peek()
  def selectPlus(df: DataFrame, cols: Array[Column]) = df.selectPlus(cols:_*)
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

  /** Reads the dataframe from persisted files, may throw error */
  def readPersistedFile(path: String): DataFrame =
    SmvUtil.readPersistedFile(app.sqlContext, path).get

  /** Saves the dataframe to disk */
  def persist(dataframe: DataFrame, path: String, generateEdd: Boolean): Unit =
    SmvUtil.persist(app.sqlContext, dataframe, path, generateEdd)

}
