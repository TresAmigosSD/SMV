/*
 * This file is licensed under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.tresamigos.smv
package edd

import org.apache.spark.sql.{DataFrame}
import org.apache.spark.sql.types._
import org.apache.spark.rdd.UnionRDD

private[smv] abstract class EddTaskGroup {
  val df: DataFrame

  val keys: Seq[String] = Seq()

  val taskList: Seq[edd.EddTask]

  def run(): DataFrame = {
    import df.sqlContext.implicits._

    /* Have to separate tasks to "old" aggregations and "new" aggregations, since
      till 1.5.1 there are 2 types of aggregations, and they can't be mixed */
    val aggCols: Seq[Either[EddTask, EddTask]] = taskList.map { t =>
      t match {
        case CntTask(_) | AvgTask(_) | MinTask(_) | MaxTask(_) | TimeMinTask(_) | TimeMaxTask(_) |
            StringMinLenTask(_) | StringMaxLenTask(_) | StringDistinctCountTask(_) =>
          Left(t)
        case _ => Right(t)
      }
    }

    val aggOlds = aggCols
      .flatMap { e =>
        e.left.toOption
      }
      .map { t =>
        t.aggCol
      }
    val aggNews = aggCols
      .flatMap { e =>
        e.right.toOption
      }
      .map { t =>
        t.aggCol
      }

    val resultCols = taskList.map { t =>
      t.resultCols
    }
    val hasKey = !keys.isEmpty

    val res = if (!hasKey) {
      // this DF only has 1 row
      if (aggOlds.isEmpty) df.agg(aggNews.head, aggNews.tail: _*).coalesce(1)
      else if (aggNews.isEmpty) df.agg(aggOlds.head, aggOlds.tail: _*).coalesce(1)
      else
        df.agg(aggNews.head, aggNews.tail: _*)
          .coalesce(1)
          .crossJoin(df.agg(aggOlds.head, aggOlds.tail: _*).coalesce(1))
    } else {
      // on row per group
      val dfGd = df
        .smvSelectPlus(smvfuncs.smvStrCat("_", keys.map { c =>
          $"$c"
        }: _*) as "groupKey")
        .groupBy("groupKey")
      if (aggOlds.isEmpty) dfGd.agg(aggNews.head, aggNews.tail: _*).coalesce(1)
      else if (aggNews.isEmpty) dfGd.agg(aggOlds.head, aggOlds.tail: _*).coalesce(1)
      else
        dfGd
          .agg(aggNews.head, aggNews.tail: _*)
          .coalesce(1)
          .smvJoinByKey(dfGd.agg(aggOlds.head, aggOlds.tail: _*).coalesce(1),
                        Seq("groupKey"),
                        SmvJoinType.Inner)
    }

    val resCached = res.cache

    val schemaStr = (if (hasKey) "groupKey: String;" else "") +
      "colName: String;" +
      "taskType: String;" +
      "taskName: String;" +
      "taskDesc: String;" +
      "valueJSON: String"

    /* since DF unionAll operation on a lot small DFs may build a large tree.
       The schema comparison on a large tree could introduce significant overhead.
       Here we convert each small DF to RDD first, then create UnionRDD, which
       is a very cheap operation */
    /* collect here before unpersist resCached */
    val resRdds = resultCols.map { rcols =>
      val cols     = if (hasKey) $"groupKey" +: rcols else rcols
      val rddArray = resCached.select(cols: _*).rdd.collect
      df.sqlContext.sparkContext.makeRDD(rddArray, 1)
    }
    resCached.unpersist()

    val resRdd = new UnionRDD(df.sqlContext.sparkContext, resRdds)

    df.sqlContext.createDataFrame(resRdd, SmvSchema.fromString(schemaStr).toStructType)
  }

}

private[smv] class EddSummary(
    override val df: DataFrame,
    override val keys: Seq[String] = Seq()
)(colNames: String*)
    extends EddTaskGroup {

  val listSeq =
    if (colNames.isEmpty) {
      df.columns.diff(keys).toSeq
    } else {
      colNames.toSet.toSeq
    }

  override val taskList = listSeq.flatMap { l =>
    val s = df(l)
    df.schema(l).dataType match {
      case _: NumericType =>
        Seq(CntTask(s), NulCntTask(s), AvgTask(s), StdDevTask(s), MinTask(s), MaxTask(s))
      case BooleanType => Seq(edd.BooleanHistogram(s))
      case TimestampType =>
        Seq(
          TimeMinTask(s),
          TimeMaxTask(s),
          YearHistogram(s),
          MonthHistogram(s),
          DoWHistogram(s),
          HourHistogram(s)
        )
      case DateType =>
        Seq(
          TimeMinTask(s),
          TimeMaxTask(s),
          YearHistogram(s),
          MonthHistogram(s),
          DoWHistogram(s)
        )
      case StringType =>
        Seq(
          CntTask(s),
          NulCntTask(s),
          StringMinLenTask(s),
          StringMaxLenTask(s),
          StringDistinctCountTask(s)
        )
      case ArrayType(_,_) => {
        println("Column \"" + df.schema(l).name + "\" has dataType ArrayType, no EDD will be built for it.")
        Seq()
      }
    }
  }

}

private[smv] class NullRate(
    override val df: DataFrame,
    override val keys: Seq[String] = Seq()
)(colNames: String*)
    extends EddTaskGroup {
  val listSeq =
    if (colNames.isEmpty)
      df.columns.toSeq
    else
      colNames.toSet.toSeq

  override val taskList = listSeq.map { l =>
    NullRateTask(df(l))
  }
}

private[smv] class EddHistogram(
    override val df: DataFrame,
    override val keys: Seq[String] = Seq()
)(histCols: HistColumn*)
    extends EddTaskGroup {

  private def createHist(histCol: edd.Hist) = {
    val s = df(histCol.colName)
    df.schema(histCol.colName).dataType match {
      case StringType =>
        if (histCol.sortByFreq) edd.StringByFreqHistogram(s)
        else edd.StringByKeyHistogram(s)
      case _: NumericType => edd.BinNumericHistogram(s, histCol.binSize)
      case BooleanType    => edd.BooleanHistogram(s)
      case t              => throw new SmvUnsupportedType(s"data type: ${t} is not supported")
    }
  }

  override val taskList = histCols.map { c =>
    c match {
      case h: edd.Hist                  => createHist(h)
      case edd.AmtHist(colName: String) => edd.AmountHistogram(df(colName))
      case t                            => throw new SmvUnsupportedType(s"data type: ${t} is not supported")
    }
  }
}
