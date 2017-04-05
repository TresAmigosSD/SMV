package org.tresamigos.smv.matcher_old

import com.rockymadden.stringmetric.similarity._
import com.rockymadden.stringmetric.phonetic._
import org.apache.spark.sql.functions._

/**
 * StringMetricUDFs is a collection of string similarity measures
 * Implemented using [[https://github.com/rockymadden/stringmetric Scala StringMetrics lib]]
 *
 * ==UDFs with Boolean returns==
 * - `soundexMatch`: ture if the Soundex of the strings matched exactly
 * ==UDFs with Float returns==
 * ===N-gram based measures===
 * - nGram2: 2-gram with formula (number of overlaped gramCnt)/max(s1.gramCnt, s2.gramCnt)
 * - nGram3: 3-gram with the same formula above
 * - diceSorensen: 2-gram with formula (2 * number of overlaped gramCnt)/(s1.gramCnt + s2.gramCnt)
 * ===Editing distance measures===
 * - levenshtein
 * - jaroWinkler
 */
object StringMetricUDFs {
  // separate function definition from the udf so we can test the function itself
  private[smv] val SoundexFn: (String, String) => Option[Boolean] = { (s1, s2) =>
    if (null == s1 || null == s2) None else SoundexMetric.compare(s1, s2)
  }

  private[smv] val NGram2Fn: (String, String) => Option[Float] = { (s1, s2) =>
    if (null == s1 || null == s2) None
    else NGramMetric(2).compare(s1, s2) map (_.toFloat)
  }

  private[smv] val NGram3Fn: (String, String) => Option[Float] = { (s1, s2) =>
    if (null == s1 || null == s2) None
    else NGramMetric(3).compare(s1, s2) map (_.toFloat)
  }

  private[smv] val DiceSorensenFn: (String, String) => Option[Float] = { (s1, s2) =>
    if (null == s1 || null == s2) None
    else DiceSorensenMetric(2).compare(s1, s2) map (_.toFloat)
  }

  private[smv] val NormalizedLevenshteinFn: (String, String) => Option[Float] = { (s1, s2) =>
    if (null == s1 || null == s2) None
    else
      LevenshteinMetric.compare(s1, s2) map { dist =>
        // normalizing to 0..1
        val maxLen = Seq(s1.length, s2.length).max
        1.0f - (dist * 1.0f / maxLen)
      }
  }

  private[smv] val JaroWinklerFn: (String, String) => Option[Float] = { (s1, s2) =>
    if (null == s1 || null == s2) None
    else JaroWinklerMetric.compare(s1, s2) map (_.toFloat)
  }

  /** UDF Return a boolean. True if Soundex of the two string are exectly matched*/
  val soundexMatch = udf(SoundexFn)

  /** UDF Return a float. 0 is no match, and 1 is full match */
  val nGram2 = udf(NGram2Fn)

  /** UDF Return a float. 0 is no match, and 1 is full match */
  val nGram3 = udf(NGram3Fn)

  /** UDF Return a float. 0 is no match, and 1 is full match */
  val diceSorensen = udf(DiceSorensenFn)

  /** UDF Return a float. 0 is no match, and 1 is full match */
  val levenshtein = udf(NormalizedLevenshteinFn)

  /** UDF Return a float. 0 is no match, and 1 is full match */
  val jaroWinkler = udf(JaroWinklerFn)
}
