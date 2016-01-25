package org.tresamigos.smv.matcher

import com.rockymadden.stringmetric.similarity.LevenshteinMetric
import org.apache.spark.sql._
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import com.rockymadden.stringmetric.phonetic._
import org.tresamigos.smv._

/**
 * TODO document the meaning of each parameter
 */
case class SmvNameMatcher(
                           exactMatchFilter:AbstractExactMatchFilter,
                           commonLevelMatcher:CommonLevelMatcher,
                           levelMatchers: List[LevelMatcher])
{
  require(!levelMatchers.isEmpty)

  private[matcher] def doMatch(df1:DataFrame, df2:DataFrame):DataFrame = {
    val ex  = if (null == exactMatchFilter) NoOpExactMatchFilter else exactMatchFilter
    // TODO: is there a way to avoid having to leak out the '_' prefix to the expression in the caller?
    val ExactMatchFilterResult(r1, r2, s1) = ex.extract(df1, df2.prefixFieldNames("_"))

    val clm = if (null == commonLevelMatcher) CommonLevelMatcherNone else commonLevelMatcher
    val j0 = clm.join(r1, r2) // join leftover unmatched data from both data frames
    // sequentially apply level matchers to the join of unmatched data
    val j1 = levelMatchers.foldLeft(j0) { (df, matcher) => matcher.addCols(df) }

    // add boolean true column to extractect results if we extracted some ids.  if we used an identity extractor, then don't add anything.
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
  def any(df: DataFrame): Column = {
    val bools: Seq[Column] = for {
      f <- df.schema.fields if f.dataType == BooleanType
    } yield df(f.name)

    if (bools.isEmpty) lit(true) else bools reduce (_ or _)
  }

}

object StringMetricUDFs {
  // separate function definition from the udf so we can test the function itself
  val SoundexFn: (String, String) => Option[Boolean] = (s1, s2) =>
  if (null  == s1 || null == s2) None else SoundexMetric.compare(s1, s2)

  val soundexMatch = udf(SoundexFn)

  val NormalizedLevenshteinFn: (String, String) => Option[Double] = (s1, s2) =>
  if (null == s1 || null == s2) None
  else LevenshteinMetric.compare(s1, s2) map { dist =>
    // normalizing to 0..1
    val maxLen = Seq(s1.length, s2.length).max
    1.0 - (dist * 1.0 / maxLen)
  }

  val levenshtein = udf(NormalizedLevenshteinFn)
}


private[matcher] case class ExactMatchFilterResult(remainingDF1:DataFrame, remainingDF2:DataFrame, extracted:DataFrame)

sealed abstract class AbstractExactMatchFilter {
  private[matcher] def extract(df1:DataFrame, df2:DataFrame):ExactMatchFilterResult
}

case class ExactMatchFilter(colName: String, private val expr:Column) extends AbstractExactMatchFilter {
  override private[matcher] def extract(df1:DataFrame, df2:DataFrame):ExactMatchFilterResult = {
    val joined = df1.join(df2, expr, "outer")

    val extracted = joined.where( joined("id").isNotNull && joined("_id").isNotNull ).select("id", "_id")

    val resultDF1 = joined.where( joined("_id").isNull ).select(df1("*"))
    val resultDF2 = joined.where( joined("id").isNull ).select(df2("*"))

    ExactMatchFilterResult(resultDF1, resultDF2, extracted)
  }
}

object NoOpExactMatchFilter extends AbstractExactMatchFilter {
  override private[matcher] def extract(df1:DataFrame, df2:DataFrame):ExactMatchFilterResult = {
    val sqc = df1.sqlContext
    val idType = df1.schema.fields.find(_.name == "id").get

    ExactMatchFilterResult(df1, df2, sqc.createDataFrame(
      sqc.sparkContext.emptyRDD[Row],
      StructType(Seq(idType, idType.copy(name = "_id")))))
  }
}

sealed abstract class CommonLevelMatcher {
  def join(df1:DataFrame, df2:DataFrame):DataFrame
}

case class CommonLevelMatcherExpression(private val expr:Column) extends CommonLevelMatcher {
  override def join(df1:DataFrame, df2:DataFrame):DataFrame = { df1.join(df2, expr) }
}

object CommonLevelMatcherNone extends CommonLevelMatcher {
  override def join(df1:DataFrame, df2:DataFrame):DataFrame = df1.join(df2)
}

sealed abstract class LevelMatcher {
  private[matcher] def getMatchColName:String
  private[matcher] def addCols(df:DataFrame):DataFrame
}

case class ExactLevelMatcher(private[matcher] val colName:String, private[matcher] val exactMatchExpression:Column) extends LevelMatcher {
  override private[matcher] def getMatchColName: String = colName
  override private[matcher] def addCols(df: DataFrame): DataFrame = df.selectPlus(exactMatchExpression.as(colName))
}

case class FuzzyLevelMatcher(
                            private[matcher] val colName:String,
                            private[matcher] val predicate:Column,
                            private[matcher] val valueExpr:Column,
                            private[matcher] val threshold: Float
                          ) extends LevelMatcher {
  override private[matcher] val getMatchColName: String = colName

  private[matcher] val valueColName: String = colName + "_Value"

  override private[matcher] def addCols(df: DataFrame): DataFrame = {
    val cond: Column =
      if (null == predicate)
        valueExpr > threshold
      else
        predicate && (valueExpr > threshold)

    df.selectPlus(cond as colName).selectPlus(valueExpr as valueColName)
  }
}
