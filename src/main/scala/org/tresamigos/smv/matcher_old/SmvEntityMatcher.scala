package org.tresamigos.smv
package matcher_old

import org.apache.spark.sql.{Column, Row, DataFrame}
import org.apache.spark.sql.types.{BooleanType, StructType, FloatType}
import org.apache.spark.sql.functions.lit
import smvfuncs._

/**
 * SmvEntityMatcher
 * Perform multiple level entity matching with exact and/or fuzzy logic
 *
 * @param exactMatchFilter top level exact match condition, if records matched no further tests will be performed
 * @param commonLevelMatcher for all levels (except top level) shared deterministic condition for narrow down the search space
 * @param levelMatchers a list of common match conditions, all of them will be tested
 */
case class SmvEntityMatcher(exactMatchFilter: AbstractExactMatchFilter,
                            commonLevelMatcher: CommonLevelMatcher,
                            levelMatchers: List[LevelMatcher]) {
  require(levelMatchers != null && levelMatchers.nonEmpty)

  /**
   * Apply `SmvEntityMatcher` to the 2 DataFrames
   *
   * @param df1 DataFrame 1 with an id column with name "id"
   * @param df2 DataFrame 2 with an id column with name "id"
   * @return a DataFrame with df1's id as "id" and df2's id as "_id" and match flags of all the
   *         levels. For levels with fuzzy logic, the matching score is also provided.
   *         A column named "MatchBitmap" also provided to summarize all the matching flags
   **/
  def doMatch(df1: DataFrame, df2: DataFrame): DataFrame = {
    require(df1 != null && df2 != null)

    val ex = if (null == exactMatchFilter) NoOpExactMatchFilter else exactMatchFilter
    // TODO: is there a way to avoid having to leak out the '_' prefix to the expression in the caller?
    val ExactMatchFilterResult(r1, r2, s1) = ex.extract(df1, df2.prefixFieldNames("_"))

    val clm = if (null == commonLevelMatcher) CommonLevelMatcherNone else commonLevelMatcher
    val j0  = clm.join(r1, r2) // join leftover unmatched data from both data frames
    // sequentially apply level matchers to the join of unmatched data
    val j1 = levelMatchers.foldLeft(j0) { (df, matcher) =>
      matcher.addCols(df)
    }

    // add boolean true column to extracted results if we extracted some ids.  if we used an identity extractor, then don't add anything.
    // add boolean false column to the joined data frame if we extracted some ids.  if we used an identity extractor, then don't add anything.
    val (s2, j2) = ex match {
      case x: ExactMatchFilter =>
        (s1.smvSelectPlus(lit(true) as x.colName), j1.smvSelectPlus(lit(false) as x.colName))
      case _ => (s1, j1)
    }

    // add the rest of the level columns to extracted results.  Set these to null.
    val s3 = levelMatchers.foldLeft(s2) { (df, matcher) =>
      matcher match {
        case m: ExactLevelMatcher =>
          df.smvSelectPlus(lit(null).cast(BooleanType) as m.getMatchColName)

        case m: FuzzyLevelMatcher =>
          df.smvSelectPlus(
            lit(null).cast(BooleanType) as m.getMatchColName,
            lit(null).cast(FloatType) as m.valueColName
          )
      }
    }

    // out of the joined data frame, select only the columns we need: ids, optional extracted column, and level columns
    val addedLevelsStageDF = j2.select(s3.columns.head, s3.columns.tail: _*)

    // return extracted results data frame + added levels data frame
    val s4 = s3.unionAll(addedLevelsStageDF)

    // minus the rows that has false for all the matcher columns
    val s5 = s4.where(any(s4))

    val allLevels = (if (exactMatchFilter == null) Nil else Seq(exactMatchFilter.colName)) ++
      levelMatchers.map { l =>
        l.getMatchColName
      }

    //add MatchBitmap column
    s5.smvSelectPlus(smvBoolsToBitmap(allLevels.head, allLevels.tail: _*).as("MatchBitmap"))
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

private[smv] case class ExactMatchFilterResult(remainingDF1: DataFrame,
                                               remainingDF2: DataFrame,
                                               extracted: DataFrame)

private[smv] sealed abstract class AbstractExactMatchFilter {
  def colName: String
  private[smv] def extract(df1: DataFrame, df2: DataFrame): ExactMatchFilterResult
}

/**
 * Specify the top-level exact match
 * @param colName level name used in the output DF
 * @param expr match logic condition Column
 **/
case class ExactMatchFilter(override val colName: String, expr: Column)
    extends AbstractExactMatchFilter {
  require(colName != null && expr.toExpr.dataType == BooleanType)

  private[smv] override def extract(df1: DataFrame, df2: DataFrame): ExactMatchFilterResult = {
    val joined = df1.join(df2, expr, "outer")

    val extracted =
      joined.where(joined("id").isNotNull && joined("_id").isNotNull).select("id", "_id")

    val resultDF1 = joined.where(joined("_id").isNull).select(df1("*"))
    val resultDF2 = joined.where(joined("id").isNull).select(df2("*"))

    ExactMatchFilterResult(resultDF1, resultDF2, extracted)
  }
}

object NoOpExactMatchFilter extends AbstractExactMatchFilter {
  override val colName = "NoOpExactMatchFilter"

  private[smv] override def extract(df1: DataFrame, df2: DataFrame): ExactMatchFilterResult = {
    val sqc    = df1.sqlContext
    val idType = df1.schema.fields.find(_.name == "id").get

    ExactMatchFilterResult(df1,
                           df2,
                           sqc.createDataFrame(sqc.sparkContext.emptyRDD[Row],
                                               StructType(Seq(idType, idType.copy(name = "_id")))))
  }
}

private[smv] sealed abstract class CommonLevelMatcher {
  private[smv] def join(df1: DataFrame, df2: DataFrame): DataFrame
}

/**
 * Specify the shared matching condition of all the levels (except the top-level exact match)
 * @param expr shared matching condition
 * @note `expr` should be in "left === right" form so that it can
 *              really help on optimize the process by reducing searching space
 **/
case class CommonLevelMatcherExpression(expr: Column) extends CommonLevelMatcher {
  //TODO: Also need to require the expression has the form * === *, otherwise there are no optimization at all
  // or an alternative, make the parameter two strings as the matched column names.
  require(expr != null && expr.toExpr.dataType == BooleanType)
  private[smv] override def join(df1: DataFrame, df2: DataFrame): DataFrame = {
    df1.join(df2, expr)
  }
}

object CommonLevelMatcherNone extends CommonLevelMatcher {
  private[smv] override def join(df1: DataFrame, df2: DataFrame): DataFrame = df1.join(df2)
}

private[smv] sealed abstract class LevelMatcher {
  private[smv] def getMatchColName: String
  private[smv] def addCols(df: DataFrame): DataFrame
}

/**
 * Level match with exact logic
 * @param colName level name used in the output DF
 * @param exactMatchExpression match logic colName
 **/
case class ExactLevelMatcher(colName: String, exactMatchExpression: Column) extends LevelMatcher {
  require(colName != null && exactMatchExpression != null)
  private[smv] override def getMatchColName: String = colName
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
case class FuzzyLevelMatcher(
    val colName: String,
    val predicate: Column,
    val valueExpr: Column,
    val threshold: Float
) extends LevelMatcher {

  require(null == predicate || predicate.toExpr.dataType == BooleanType,
          "The predicate parameter should be null or a boolean column")

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

    df.smvSelectPlus(cond as colName).smvSelectPlus(valueExpr as valueColName)
  }
}
