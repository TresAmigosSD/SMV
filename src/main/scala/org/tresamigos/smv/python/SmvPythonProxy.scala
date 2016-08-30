package org.tresamigos.smv.python

import org.apache.spark._, sql._
import org.tresamigos.smv._

/** Use an object instead of a module so interface using py4j is more straight-forward */
class SmvPythonProxy {
  def init(sqlContext: SQLContext): SmvApp = init(Array("-m", "None"), sqlContext)

  def init(args: Array[String], sqlContext: SQLContext) =
    SmvApp.init(args, Option(sqlContext.sparkContext), Option(sqlContext))

  def readPersistedFile(sqlContext: SQLContext, path: String): DataFrame =
    SmvUtil.readPersistedFile(sqlContext, path).get

  def persist(sqlContext: SQLContext, dataframe: DataFrame, path: String, generateEdd: Boolean) =
    SmvUtil.persist(sqlContext, dataframe, path, generateEdd)

  def peek(df: DataFrame) = df.peek()
  def selectPlus(df: DataFrame, cols: Array[Column]) = df.selectPlus(cols:_*)
}
