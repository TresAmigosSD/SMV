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

  /** For an Array column create a String column with the Array values */
  def smvArrayCat(sep: String, col: Column, fn: Any => String = (x => x.toString)): Column = {
    val catF = {a:Seq[Any] =>
      a.map{
        case null => ""
        case s => fn(s)
      }.mkString(sep)
    }

    udf(catF).apply(col).as(s"smvArrayCat(${col})")
  }

  /**
   * Creating unique id from the primary key list.
   *
   * Return "Prefix" + MD5 Hex string(size 32 string) as the unique key
   *
   * MD5's collisions rate on real data records could be ignored based on the following discussion.
   *
   * https://marc-stevens.nl/research/md5-1block-collision/
   * The shortest messages have the same MD5 are 512-bit (64-byte) messages as below
   *
   * 4dc968ff0ee35c209572d4777b721587d36fa7b21bdc56b74a3dc0783e7b9518afbfa200a8284bf36e8e4b55b35f427593d849676da0d1555d8360fb5f07fea2
   * and the (different by two bits)
   * 4dc968ff0ee35c209572d4777b721587d36fa7b21bdc56b74a3dc0783e7b9518afbfa202a8284bf36e8e4b55b35f427593d849676da0d1d55d8360fb5f07fea2
   * both have MD5 hash
   * 008ee33a9d58b51cfeb425b0959121c9
   *
   * There are other those pairs, but all carefully constructed.
   * Theoretically the random collisions will happen on data size approaching 2^64 (since MD5 has
   * 128-bit), which is much larger than the number of records we deal with (a billion is about 2^30)
   * There for using MD5 to hash primary key columns is good enough for creating an unique key
   */
  def smvHashKey(prefix: String, cols: Column*): Column = {
    val allcols = lit(prefix) +: cols
    smvStrCat(lit(prefix), md5(smvStrCat(allcols: _*))) as s"smvHashKey(${prefix}, ${cols})"
  }

  def smvHashKey(cols: Column*): Column = smvHashKey("", cols: _*)
}
