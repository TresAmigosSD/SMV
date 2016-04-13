package org.tresamigos.smv.matcher

import com.rockymadden.stringmetric.similarity._
import org.apache.spark.sql._
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.DataFrame
import com.rockymadden.stringmetric.phonetic._
import org.tresamigos.smv._

/**
 * TODO document the meaning of each parameter
 */
case class SmvEntityMatcher(
                           exactMatchFilter:AbstractExactMatchFilter,
                           commonLevelMatcher:CommonLevelMatcher,
                           levelMatchers: List[LevelMatcher])
{
  require(levelMatchers != null && levelMatchers.nonEmpty)

  /**
   * TODO: API doc for this public method
   **/
  def doMatch(df1:DataFrame, df2:DataFrame):DataFrame = {
    require(df1 != null && df2 != null)

    val ex  = if (null == exactMatchFilter) NoOpExactMatchFilter else exactMatchFilter
    // TODO: is there a way to avoid having to leak out the '_' prefix to the expression in the caller?
    val ExactMatchFilterResult(r1, r2, s1) = ex.extract(df1, df2.prefixFieldNames("_"))

    val clm = if (null == commonLevelMatcher) CommonLevelMatcherNone else commonLevelMatcher
    val j0 = clm.join(r1, r2) // join leftover unmatched data from both data frames
    // sequentially apply level matchers to the join of unmatched data
    val j1 = levelMatchers.foldLeft(j0) { (df, matcher) => matcher.addCols(df) }

    // add boolean true column to extracted results if we extracted some ids.  if we used an identity extractor, then don't add anything.
    // add boolean false column to the joined data frame if we extracted some ids.  if we used an identity extractor, then don't add anything.
    val (s2, j2) = ex match {
      case x: ExactMatchFilter =>
        (s1.selectPlus(lit(true) as x.colName), j1.selectPlus(lit(false) as x.colName))
      case _ => (s1, j1)
    }

    // add the rest of the level columns to extracted results.  Set these to null.
    val s3 = levelMatchers.foldLeft(s2) { (df, matcher) =>
      matcher match {
        case m: ExactLevelMatcher =>
          df.selectPlus(lit(null).cast(BooleanType) as m.getMatchColName)

        case m: FuzzyLevelMatcher =>
          df.selectPlus(
            lit(null).cast(BooleanType) as m.getMatchColName,
            lit(null).cast(FloatType) as m.valueColName
          )
      }
    }

    // out of the joined data frame, select only the columns we need: ids, optional extracted column, and level columns
    val addedLevelsStageDF = j2.select(s3.columns.head, s3.columns.tail:_*)

    // return extracted results data frame + added levels data frame
    val s4 = s3.unionAll(addedLevelsStageDF)

    // minus the rows that has false for all the matcher columns
    s4.where(any(s4))
  }

  // Returns a predicate that would evaluate to true if any of the
  // boolean columns in the data frame evaluates to true.
  //
  // If the data frame has no boolean types then this function returns
  // a column that's always true.  Note that this is not the true
  // zero-value for the 'or' operator (the zero is false), and that is
  // because the return value of 'any' is usually used to filter a
  // data frame, and we would like that behavior to be a no-op instead
  // of dropping all the rows in the data frame.
  // TODO: In case that the original DFs have boolean columns, this method is dangerous!
  private def any(df: DataFrame): Column = {
    val bools: Seq[Column] = for {
      f <- df.schema.fields if f.dataType == BooleanType
    } yield df(f.name)

    if (bools.isEmpty) lit(true) else bools reduce (_ or _)
  }

}

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
  private[smv] val SoundexFn: (String, String) => Option[Boolean] = {(s1, s2) =>
    if (null  == s1 || null == s2) None else SoundexMetric.compare(s1, s2)
  }

  private[smv] val NGram2Fn: (String, String) => Option[Float] = {(s1, s2) =>
    if (null == s1 || null == s2) None
    else NGramMetric(2).compare(s1, s2) map (_.toFloat)
  }

  private[smv] val NGram3Fn: (String, String) => Option[Float] = {(s1, s2) =>
    if (null == s1 || null == s2) None
    else NGramMetric(3).compare(s1, s2) map (_.toFloat)
  }

  private[smv] val DiceSorensenFn: (String, String) => Option[Float] = {(s1, s2) =>
    if (null == s1 || null == s2) None
    else DiceSorensenMetric(2).compare(s1, s2) map (_.toFloat)
  }

  private[smv] val NormalizedLevenshteinFn: (String, String) => Option[Float] = {(s1, s2) =>
    if (null == s1 || null == s2) None
    else LevenshteinMetric.compare(s1, s2) map { dist =>
      // normalizing to 0..1
      val maxLen = Seq(s1.length, s2.length).max
      1.0f - (dist * 1.0f / maxLen)
    }
  }

  private[smv] val JaroWinklerFn: (String, String) => Option[Float] = {(s1, s2) =>
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


private[smv] case class ExactMatchFilterResult(remainingDF1:DataFrame, remainingDF2:DataFrame, extracted:DataFrame)

private[smv] sealed abstract class AbstractExactMatchFilter {
  private[smv] def extract(df1:DataFrame, df2:DataFrame):ExactMatchFilterResult
}

case class ExactMatchFilter(colName: String, expr:Column) extends AbstractExactMatchFilter {
  require(colName != null && expr.toExpr.dataType == BooleanType)

  private[smv] override def extract(df1:DataFrame, df2:DataFrame):ExactMatchFilterResult = {
    val joined = df1.join(df2, expr, "outer")

    val extracted = joined.where( joined("id").isNotNull && joined("_id").isNotNull ).select("id", "_id")

    val resultDF1 = joined.where( joined("_id").isNull ).select(df1("*"))
    val resultDF2 = joined.where( joined("id").isNull ).select(df2("*"))

    ExactMatchFilterResult(resultDF1, resultDF2, extracted)
  }
}

object NoOpExactMatchFilter extends AbstractExactMatchFilter {
  private[smv] override def extract(df1:DataFrame, df2:DataFrame):ExactMatchFilterResult = {
    val sqc = df1.sqlContext
    val idType = df1.schema.fields.find(_.name == "id").get

    ExactMatchFilterResult(df1, df2, sqc.createDataFrame(
      sqc.sparkContext.emptyRDD[Row],
      StructType(Seq(idType, idType.copy(name = "_id")))))
  }
}

private[smv] sealed abstract class CommonLevelMatcher {
  private[smv] def join(df1:DataFrame, df2:DataFrame):DataFrame
}

case class CommonLevelMatcherExpression(expr:Column) extends CommonLevelMatcher {
  //TODO: Also need to require the expression has the form * === *, otherwise there are no optimization at all
  // or an alternative, make the parameter two strings as the matched column names. 
  require(expr != null && expr.toExpr.dataType == BooleanType)
  private[smv] override def join(df1:DataFrame, df2:DataFrame):DataFrame = { df1.join(df2, expr) }
}

object CommonLevelMatcherNone extends CommonLevelMatcher {
  private[smv] override def join(df1:DataFrame, df2:DataFrame):DataFrame = df1.join(df2)
}

private[smv] sealed abstract class LevelMatcher {
  private[smv] def getMatchColName:String
  private[smv] def addCols(df:DataFrame):DataFrame
}

case class ExactLevelMatcher(colName:String, exactMatchExpression:Column) extends LevelMatcher {
  require(colName != null && exactMatchExpression != null)
  private[smv] override def getMatchColName: String = colName
  private[smv] override def addCols(df: DataFrame): DataFrame = df.selectPlus(exactMatchExpression.as(colName))
}

case class FuzzyLevelMatcher(
                            val colName:String,
                            val predicate:Column,
                            val valueExpr:Column,
                            val threshold: Float
                          ) extends LevelMatcher {

  require(null == predicate || predicate.toExpr.dataType == BooleanType, "The predicate parameter should be null or a boolean column")

  require(
    colName != null &&
    valueExpr != null && valueExpr.toExpr.dataType == FloatType)

  private[smv] override val getMatchColName: String = colName

  private[smv] val valueColName: String = colName + "_Value"

  private[smv] override def addCols(df: DataFrame): DataFrame = {
    val cond: Column =
      if (null == predicate)
        valueExpr > threshold
      else
        predicate && (valueExpr > threshold)

    df.selectPlus(cond as colName).selectPlus(valueExpr as valueColName)
  }
}
