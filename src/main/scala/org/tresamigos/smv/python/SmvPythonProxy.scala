package org.tresamigos.smv.python

import org.apache.spark.sql._
import org.tresamigos.smv._

/** Use an object instead of a module so interface using py4j is more straight-forward */
class SmvPythonProxy {
  def peek(df: DataFrame) = df.peek()
  def selectPlus(df: DataFrame, cols: Array[Column]) = df.selectPlus(cols:_*)
}
