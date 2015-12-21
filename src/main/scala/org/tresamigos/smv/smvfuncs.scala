package org.tresamigos.smv

import org.apache.spark.sql._, functions._

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

  /** Convenience method */
  def smvCountFalse(cond: Column): Column = smvCountTrue(!cond)
}
