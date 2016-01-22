package org.tresamigos.smv.matcher

import com.rockymadden.stringmetric.similarity.LevenshteinMetric
import org.tresamigos.smv._
import org.apache.spark.sql.{Column, DataFrame}
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import com.rockymadden.stringmetric.phonetic._


case class SmvNameMatcher(
                           exactMatchFilter:AbstractExactMatchFilter,
                           commonLevelMatcher:CommonLevelMatcher,
                           levelMatchers: List[LevelMatcher])
{
  private val idColNames = List("id", "_id")

  private[matcher] def doMatch(df1:DataFrame, df2:DataFrame):DataFrame = {
    val ex = Option(exactMatchFilter).getOrElse(NoOpExactMatchFilter)
    val clm = Option(commonLevelMatcher).getOrElse(CommonLevelMatcherNone)

    val _df2 = df2.prefixFieldNames("_")

    // case class with: extracted ids, reduced df1 & df2
    val extractRes = ex.extract(df1, _df2)

    // reduced df1 join to reduced df2 produces the joined data frame
    val joined = clm.join(extractRes.remainingDF1, extractRes.remainingDF2)

//    println(joined.collect())

    // joined data frame + all level columns
//    var joinedWithAddedLevelsDF = Function.chain(levelMatchers.map(matcher => matcher.addCols _))(joined)
    var joinedWithAddedLevelsDF = levelMatchers.foldLeft(joined)((acc, matcher) => matcher.addCols(acc))

    // extracted ids
    var extractedResultStageDF = extractRes.extracted

    // add boolean true column to extractect results if we extracted some ids.  if we used an identity extractor, then don't add anything.
    // add boolean false column to the joined data frame if we extracted some ids.  if we used an identity extractor, then don't add anything.
    ex match {
      case ExactMatchFilter(_, _) => {
        extractedResultStageDF = extractedResultStageDF.selectPlus(lit(true).as(ex.asInstanceOf[ExactMatchFilter].colName))
        joinedWithAddedLevelsDF = joinedWithAddedLevelsDF.selectPlus(lit(false).as(ex.asInstanceOf[ExactMatchFilter].colName))
      }
      case _ =>
    }

    // add the rest of the level columns to extracted results.  Set these to null.
    levelMatchers.foreach {
      case matcher@ExactLevelMatcher(_, _) => {
        extractedResultStageDF = extractedResultStageDF.selectPlus(
          lit(null).cast(BooleanType).as(matcher.getMatchColName)
        )
      }

      case matcher@FuzzyLevelMatcher(_, _, _, _) => {
        extractedResultStageDF = extractedResultStageDF.selectPlus(
          lit(null).cast(BooleanType).as(matcher.getMatchColName),
          lit(null).cast(FloatType).as(matcher.asInstanceOf[FuzzyLevelMatcher].getFuzzyColName)
        )
      }
    }

    // out of the joined data frame, select only the columns we need: ids, optional extracted column, and level columns
    val addedLevelsStageCols = extractedResultStageDF.columns map (colName => joinedWithAddedLevelsDF(colName))
    val addedLevelsStageDF = joinedWithAddedLevelsDF.select(addedLevelsStageCols:_*)

    // return extracted results data frame + added levels data frame
    extractedResultStageDF.unionAll(addedLevelsStageDF)

    // TODO: remove rows that evalutes to false for all filters
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

case object NoOpExactMatchFilter extends AbstractExactMatchFilter {
  override private[matcher] def extract(df1:DataFrame, df2:DataFrame):ExactMatchFilterResult = {
    ExactMatchFilterResult(df1, df2, df1.join(df2).select("id", "_id").limit(0))
  }
}

abstract class CommonLevelMatcher {
  def join(df1:DataFrame, df2:DataFrame):DataFrame
}

case class CommonLevelMatcherExpression(private val expr:Column) extends CommonLevelMatcher {
  override def join(df1:DataFrame, df2:DataFrame):DataFrame = { df1.join(df2, expr) }
}

object CommonLevelMatcherNone extends CommonLevelMatcher {
  override def join(df1:DataFrame, df2:DataFrame):DataFrame = df1.join(df2)
}

abstract class LevelMatcher {
  private[matcher] def getMatchColName:String
  private[matcher] def addCols(df:DataFrame):DataFrame
}

case class ExactLevelMatcher(private[matcher] val colName:String, private[matcher] val exactMatchExpression:Column) extends LevelMatcher {
  override private[matcher] def getMatchColName: String = colName
  override private[matcher] def addCols(df: DataFrame): DataFrame = df.selectPlus(exactMatchExpression.as(colName))
}

case class FuzzyLevelMatcher(
                            private[matcher] val colName:String,
                            private[matcher] val exactMatchExpression:Column,
                            private[matcher] val stringDistanceExpression:Column,
                            private[matcher] val threshold:Float
                          ) extends LevelMatcher {
  override private[matcher] def getMatchColName: String = colName

  private[matcher] def getFuzzyColName: String = colName + "_Value"

  override private[matcher] def addCols(df: DataFrame): DataFrame = {
    df.selectPlus((stringDistanceExpression > threshold).as(getFuzzyColName))

    if (exactMatchExpression == null) {
      df.selectPlus(stringDistanceExpression > threshold).as(colName)
    } else {
      df.selectPlus((exactMatchExpression && (stringDistanceExpression > threshold)).as(colName))
    }
  }
}
