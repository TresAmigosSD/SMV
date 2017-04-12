package org.tresamigos.smv

import org.apache.spark.sql.{Column, Row}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import com.rockymadden.stringmetric.similarity._

/**
 * Commonly used functions
 *
 * @since 1.5
 */
object smvfuncs {

  /**
   * smvFirst: by default return null if the first record is null
   *
   * Since Spark 1.5 "first" will return the first non-null value, we have to create
   * our version smvFirst which to retune the real first value, even if it's null.
   * The alternative form will try to return the first non-null value
   *
   * Spark 2.1 enhanced the first function to take nonNull parameter.
   * We can simply forward the call and maintain the old interface.
   *
   * @param c        the column
   * @param nonNull  switches whether the function will try to find the first non-null value
   *
   * @group agg
   **/
  def smvFirst(c: Column, nonNull: Boolean = false) = first(c, nonNull)

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
    val catCol = smvStrCat(cols.map { c =>
      c.cast(StringType)
    }: _*)
    countDistinct(catCol)
  }

  def smvCountDistinctWithNull(colN: String, colNs: String*): Column = {
    val cols = (colN +: colNs).map { cn =>
      new Column(cn)
    }
    smvCountDistinctWithNull(cols: _*)
  }

  val boolsToBitmap = (r: Row) => {
    r.toSeq.map({ case true => '1'; case false | null => '0' }).mkString
  }

  /** Coalesce boolean columns into a String bitmap  **/
  def smvBoolsToBitmap(boolColumns: Column*) = {
    udf(boolsToBitmap).apply(struct(boolColumns: _*))
  }

  /** Coalesce boolean columns into a String bitmap  **/
  def smvBoolsToBitmap(headColumnName: String, tailColumnNames: String*) = {
    udf(boolsToBitmap).apply(struct(headColumnName, tailColumnNames: _*))
  }

  /** Spark 1.6 will have collect_set aggregation function.*/
  def smvCollectSet(c: Column, dt: DataType): Column = {

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
        throw new SmvUnsupportedType("collectSet unsupported type: " + dt.typeName)
      }
    }
  }

  @deprecated("Replaced by smvCollectSet(col, datatype)", "2.1")
  def collectSet(dt: DataType)(c: Column): Column = smvCollectSet(c, dt)

  /** For an Array column create a String column with the Array values */
  def smvArrayCat(sep: String, col: Column, fn: Any => String): Column = {
    val catF = { a: Seq[Any] =>
      a.map {
          case null => ""
          case s    => fn(s)
        }
        .mkString(sep)
    }

    udf(catF).apply(col).as(s"smvArrayCat(${col})")
  }

  def smvArrayCat(sep: String, col: Column): Column = smvArrayCat(sep, col, {x:Any => x.toString})

  /** True if any of the columns is not null */
  def smvHasNonNull(columns: Column*) = columns.foldRight(lit(false))((c, acc) => acc || c.isNotNull)

  /**
   * Patch Spark's `concat` and `concat_ws` to treat null as empty string in concatenation.
   **/
  def smvStrCat(columns: Column*) =
    when(smvHasNonNull(columns: _*), concat(columns.map { c =>
      coalesce(c, lit(""))
    }: _*)).otherwise(lit(null)).alias(s"smvStrCat(${columns.mkString(", ")})")

  def smvStrCat(sep: String, columns: Column*) =
    when(smvHasNonNull(columns: _*), concat_ws(sep, columns.map(c => coalesce(c, lit(""))): _*))
      .otherwise(lit(null)).alias(s"smvStrCat($sep, ${columns.mkString(", ")})")

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

  /**
   * Calculate N-gram (N=2) distance between 2 string typed columns
   * Returns a float. 0 is no match, and 1 is full match
   *
   * Algorithm reference: https://en.wikipedia.org/wiki/N-gram
   * Library reference: https://github.com/rockymadden/stringmetric
   */
  def nGram2(c1: Column, c2: Column) = {
    val NGram2Fn: (String, String) => Option[Float] = { (s1, s2) =>
      if (null == s1 || null == s2) None
      else NGramMetric(2).compare(s1, s2) map (_.toFloat)
    }
    udf(NGram2Fn).apply(c1, c2).alias(s"nGram2(${c1}, ${c2})")
  }

  /**
   * Calculate N-gram (N=3) distance between 2 string typed columns
   * Returns a float. 0 is no match, and 1 is full match
   *
   * Algorithm reference: https://en.wikipedia.org/wiki/N-gram
   * Library reference: https://github.com/rockymadden/stringmetric
   */
  def nGram3(c1: Column, c2: Column) = {
    val NGram3Fn: (String, String) => Option[Float] = { (s1, s2) =>
      if (null == s1 || null == s2) None
      else NGramMetric(3).compare(s1, s2) map (_.toFloat)
    }
    udf(NGram3Fn).apply(c1, c2).alias(s"nGram3(${c1}, ${c2})")
  }

  /**
   * Calculate Dice-Sorensen distance between 2 string typed columns
   * Returns a float. 0 is no match, and 1 is full match
   *
   * Algorithm reference: https://en.wikipedia.org/wiki/S%C3%B8rensen%E2%80%93Dice_coefficient
   * Library reference: https://github.com/rockymadden/stringmetric
   */
  def diceSorensen(c1: Column, c2: Column) = {
    val DiceSorensenFn: (String, String) => Option[Float] = { (s1, s2) =>
      if (null == s1 || null == s2) None
      else DiceSorensenMetric(2).compare(s1, s2) map (_.toFloat)
    }
    udf(DiceSorensenFn).apply(c1, c2).alias(s"DiceSorensen(${c1}, ${c2})")
  }

  /**
   * Calculate Normalized Levenshtein distance between 2 string typed columns
   * Returns a float. 0 is no match, and 1 is full match
   *
   * Algorithm reference: https://en.wikipedia.org/wiki/Levenshtein_distance
   * Library reference: https://github.com/rockymadden/stringmetric
   */
  def normlevenshtein(c1: Column, c2: Column) = {
    val NormalizedLevenshteinFn: (String, String) => Option[Float] = { (s1, s2) =>
      if (null == s1 || null == s2) None
      else
        LevenshteinMetric.compare(s1, s2) map { dist =>
          // normalizing to 0..1
          val maxLen = Seq(s1.length, s2.length).max
          1.0f - (dist * 1.0f / maxLen)
        }
    }
    udf(NormalizedLevenshteinFn).apply(c1, c2).alias(s"NormLevenshtein(${c1}, ${c2})")
  }

  /**
   * Calculate Jaro–Winkler distance between 2 string typed columns
   * Returns a float. 0 is no match, and 1 is full match
   *
   * Algorithm reference: https://en.wikipedia.org/wiki/Jaro%E2%80%93Winkler_distance
   * Library reference: https://github.com/rockymadden/stringmetric
   */
  def jaroWinkler(c1: Column, c2: Column) = {
    val JaroWinklerFn: (String, String) => Option[Float] = { (s1, s2) =>
      if (null == s1 || null == s2) None
      else JaroWinklerMetric.compare(s1, s2) map (_.toFloat)
    }
    udf(JaroWinklerFn).apply(c1, c2).alias(s"JaroWinkler(${c1}, ${c2})")
  }

}
