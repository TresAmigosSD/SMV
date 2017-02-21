package org.tresamigos.smv.matcher2

import org.apache.spark.sql._
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.catalyst.expressions.EqualTo
import org.apache.spark.sql.DataFrame
import org.tresamigos.smv._, smvfuncs._

/**
 * SmvEntityMatcher
 * Perform multiple level entity matching with exact and/or fuzzy logic
 *
 * @param leftId id column name of left DF (df1)
 * @param rightId id column name of right DF (df2)
 * @param exactMatchFilter exact match condition, if records matched no further tests will be performed
 * @param groupCondition for exact match leftovers, a deterministic condition for narrow down the search space
 * @param levelLogics a list of level match conditions (always weaker than exactMatchFilter), all of them will be tested
 */
case class SmvEntityMatcher(leftId: String, rightId: String,
                           exactMatchFilter:ExactMatchPreFilter,
                           groupCondition:GroupCondition,
                           levelLogics: Seq[LevelLogic])
{
  require(levelLogics != null && levelLogics.nonEmpty)

  private val allLevels = (if(exactMatchFilter == null) Nil else Seq(exactMatchFilter.colName)) ++
      levelLogics.map{l => l.colName}
  private val allMatcherCols = allLevels ++
      levelLogics.collect{case l:FuzzyLogic => l.valueColName}

  /**
   * Apply `SmvEntityMatcher` to the 2 DataFrames
   *
   * @param df1 DataFrame 1 with an id column with name "id"
   * @param df2 DataFrame 2 with an id column with name "id"
   * @param keepOriginalCols whether to keep all input columns of df1 and df2, default true
   * @return a DataFrame with df1's id and df2's id and match flags of all the
   *         levels. For levels with fuzzy logic, the matching score is also provided.
   *         A column named "MatchBitmap" also provided to summarize all the matching flags.
   *         When keepOriginalCols is true, input columns are also kept
   **/
  def doMatch(df1:DataFrame, df2:DataFrame, keepOriginalCols: Boolean = true):DataFrame = {
    require(df1 != null && df2 != null)

    // Use the exactMatchFilter to filter the data to 3 parts
    //  - fullMatched: fully matched records joined together
    //  - r1: df1's records which can't match under exactMatchFilter logic
    //  - r2: df2's records which can't match under exactMatchFilter logic
    val ExactMatchPreFilterResult(r1, r2, fullMatched) =
      if (null == exactMatchFilter) ExactMatchPreFilterResult(df1, df2, null)
      else exactMatchFilter.extract(df1, df2, leftId, rightId)

    val rJoined = if (null == groupCondition) r1.join(r2) else groupCondition.join(r1, r2)

    // sequentially apply level logics to the join of unmatched data
    // remove rows with false for all the matcher columns
    val levelMatched = levelLogics.
      foldLeft(rJoined) { (df, matcher) => matcher.addCols(df) }.
      where(levelLogics.map(l => col(l.colName)).reduce(_ || _))

    // Union fullMatched with levelMatched
    val allMatched = {
      if (null == fullMatched)
        levelMatched
      else
        fullMatched.
          smvUnion(levelMatched).
          withColumn(exactMatchFilter.colName, coalesce(col(exactMatchFilter.colName), lit(false)))
    }

    //add MatchBitmap column
    val res = allMatched.smvSelectPlus(smvBoolsToBitmap(allLevels.head, allLevels.tail: _*).as("MatchBitmap"))

    if (keepOriginalCols) res
    else res.select(
      leftId,
      Seq(rightId) ++
      allMatcherCols ++
      Seq("MatchBitmap"): _*
    )
  }
}

private[smv] case class ExactMatchPreFilterResult(remainingDF1:DataFrame, remainingDF2:DataFrame, extracted:DataFrame)

/**
 * Specify the top-level exact match
 * @param colName level name used in the output DF
 * @param expr match logic condition Column
 **/
case class ExactMatchPreFilter(val colName: String, expr:Column) {
  require(colName != null && expr.toExpr.dataType == BooleanType)

  private[smv] def extract(df1:DataFrame, df2:DataFrame, leftId: String, rightId: String):ExactMatchPreFilterResult = {
    val joined = df1.join(df2, expr, "outer")

    val extracted = joined.where( joined(leftId).isNotNull && joined(rightId).isNotNull ).
      smvSelectPlus(lit(true) as colName)

    val resultDF1 = joined.where( joined(rightId).isNull ).select(df1("*"))
    val resultDF2 = joined.where( joined(leftId).isNull ).select(df2("*"))

    ExactMatchPreFilterResult(resultDF1, resultDF2, extracted)
  }
}

/**
 * Specify the shared matching condition of all the levels (except the top-level exact match)
 * @param expr shared matching condition
 * @note `expr` should be in "left === right" form so that it can
 *              really help on optimize the process by reducing searching space
 **/
case class GroupCondition(expr:Column) {
  expr.toExpr match {
    case EqualTo(_, _) => Unit
    case _ => throw new SmvUnsupportedType("Expression should be in left === right form")
  }
  private[smv] def join(df1:DataFrame, df2:DataFrame):DataFrame = df1.join(df2, expr)
}

private[smv] sealed abstract class LevelLogic {
  private[smv] def colName:String
  private[smv] def addCols(df:DataFrame):DataFrame
}

/**
 * Level match with exact logic
 * @param colName level name used in the output DF
 * @param exactMatchExpression match logic colName
 **/
case class ExactLogic(override val colName:String, exactMatchExpression:Column) extends LevelLogic {
  require(colName != null && exactMatchExpression != null)
  private[smv] override def addCols(df: DataFrame): DataFrame =
    df.smvSelectPlus(exactMatchExpression.as(colName))
}

/**
 * Level match with fuzzy logic
 * @param colName level name used in the output DF
 * @param predicate a condition column, no match if this condition evaluated as false
 * @param valueExpr a value column, which typically return a score, higher score means higher chance of matching
 * @param threshold No match if the evaluated `valueExpr` < this value
 **/
case class FuzzyLogic(
                            override val colName:String,
                            val predicate:Column,
                            val valueExpr:Column,
                            val threshold: Float
                          ) extends LevelLogic {

  require(null == predicate || predicate.toExpr.dataType == BooleanType, "The predicate parameter should be null or a boolean column")

  private[smv] val valueColName: String = colName + "_Value"

  private[smv] override def addCols(df: DataFrame): DataFrame = {
    val cond: Column =
      if (null == predicate)
        valueExpr > threshold
      else
        predicate && (valueExpr > threshold)

    df.smvSelectPlus(cond as colName).smvSelectPlus(valueExpr as valueColName)
  }
}
