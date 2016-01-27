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
    val hashCol = cols.map{c => coalesce(crc32(c.cast(StringType)), crc32(lit("null")))}.reduce{(l, r) => l + r}
    countDistinct(hashCol)
  }

  def smvCountDistinctWithNull(colN: String, colNs: String*): Column = {
    val cols = (colN +: colNs).map{cn => new Column(cn)}
    smvCountDistinctWithNull(cols: _*)
  }
}
