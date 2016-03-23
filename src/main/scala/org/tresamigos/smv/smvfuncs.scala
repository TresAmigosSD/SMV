package org.tresamigos.smv

import org.apache.spark.sql._, functions._, types._

/**
 * Commonly used functions
 *
 * @since 1.5
 */
object smvfuncs {
  /**
   * Aggregate function that counts the number of rows satisfying a given condition.
   */
  def smvCountTrue(cond: Column): Column = count(when(cond, lit(1)).otherwise(lit(null)))

  /** Count non-null false values */
  def smvCountFalse(cond: Column): Column = smvCountTrue(!cond)

  /** Count number of null values */
  def smvCountNull(cond: Column): Column = count(when(cond.isNull, lit(1)).otherwise(lit(null)))

  /** Count number of distinct values including null */
  def smvCountDistinctWithNull(cols: Column*): Column = {
    val catCol = smvStrCat(cols.map{c => c.cast(StringType)}: _*)
    countDistinct(catCol)
  }

  def smvCountDistinctWithNull(colN: String, colNs: String*): Column = {
    val cols = (colN +: colNs).map{cn => new Column(cn)}
    smvCountDistinctWithNull(cols: _*)
  }

  val boolsToBitmap = (r:Row) => {
    r.toSeq.map( { case true => '1'; case false | null => '0' } ).mkString
  }

  /** Coalesce boolean columns into a String bitmap  **/
  def smvBoolsToBitmap(boolColumns:Column*) = {
    udf(boolsToBitmap).apply(struct(boolColumns:_*))
  }

  /** Coalesce boolean columns into a String bitmap  **/
  def smvBoolsToBitmap(headColumnName:String, tailColumnNames: String*) = {
    udf(boolsToBitmap).apply(struct(headColumnName, tailColumnNames:_*))
  }

  /** Spark 1.6 will have collect_set aggregation function.*/
  def collectSet(dt: DataType)(c: Column): Column = {

    dt match {
      case StringType => {
        val toKeys = udf((m: Map[String, Long]) => m.keys.toSeq)
        toKeys(histStr(c)) as s"collectSet($c)"
      }

      case IntegerType => {
        val toKeys = udf((m: Map[Integer, Long]) => m.keys.toSeq)
        toKeys(histInt(c)) as s"collectSet($c)"
      }

      case BooleanType => {
        val toKeys = udf((m: Map[Boolean, Long]) => m.keys.toSeq)
        toKeys(histBoolean(c)) as s"collectSet($c)"
      }

      case DoubleType => {
        val toKeys = udf((m: Map[Double, Long]) => m.keys.toSeq)
        toKeys(histDouble(c)) as s"collectSet($c)"
      }
      case _ => {
        throw new IllegalArgumentException("collectSet unsupported type: " + dt.typeName)
      }
    }
  }
}
