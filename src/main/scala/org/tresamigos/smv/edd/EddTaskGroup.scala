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

package org.tresamigos.smv.edd

import org.tresamigos.smv._
import org.apache.spark.sql.{DataFrame, Column}
import org.apache.spark.sql.types._
import org.apache.spark.rdd.UnionRDD


private[smv] abstract class EddTaskGroup {
  val df: DataFrame
  val taskList: Seq[edd.EddTask]
  def run(): DataFrame = {
    /* Have to separate tasks to "old" aggregations and "new" aggregations, since
      till 1.5.1 there are 2 types of aggregations, and they can't be mixed */
    val aggCols: Seq[Either[EddTask, EddTask]] = taskList.map{t => t match {
      case CntTask(_)|AvgTask(_)|MinTask(_)|MaxTask(_)|TimeMinTask(_)|TimeMaxTask(_)|StringMinLenTask(_)|StringMaxLenTask(_)|StringDistinctCountTask(_) => Left(t)
      case _ => Right(t)
    }}

    val aggOlds = aggCols.flatMap{e => e.left.toOption}.map{t => t.aggCol}
    val aggNews = aggCols.flatMap{e => e.right.toOption}.map{t => t.aggCol}

    val resultCols = taskList.map{t => t.resultCols}

    // this DF only has 1 row
    val res =
      if (aggOlds.isEmpty) df.agg(aggNews.head, aggNews.tail: _*).coalesce(1)
      else if (aggNews.isEmpty) df.agg(aggOlds.head, aggOlds.tail: _*).coalesce(1)
      else df.agg(aggNews.head, aggNews.tail: _*).coalesce(1).join(df.agg(aggOlds.head, aggOlds.tail: _*).coalesce(1))

    val resCached = res.cache

    val schema = SmvSchema.fromString(
      "colName: String;" +
      "taskType: String;" +
      "taskName: String;" +
      "taskDesc: String;" +
      "valueJSON: String"
    )

    /* since DF unionAll operation on a lot small DFs may build a large tree.
       The schema comparison on a large tree could introduce significant overhead.
       Here we convert each small DF to RDD first, then create UnionRDD, which
       is a very cheap operation */
    /* collect here before unpersist resCached */
    val resRdds = resultCols.map{rcols =>
      val rddArray=resCached.select(rcols: _*).rdd.collect
      df.sqlContext.sparkContext.makeRDD(rddArray, 1)
    }
    resCached.unpersist()

    val resRdd = new UnionRDD(df.sqlContext.sparkContext, resRdds)

    df.sqlContext.createDataFrame(resRdd, schema.toStructType)
  }

}

private[smv] class EddSummary(override val df: DataFrame)(colNames: String*) extends EddTaskGroup {
  val listSeq =
    if (colNames.isEmpty)
      df.columns.toSeq
    else
      colNames.toSet.toSeq

  override val taskList = listSeq.flatMap{ l =>
    val s = df(l)
    df.schema(l).dataType match {
      case _: NumericType => Seq(CntTask(s), AvgTask(s), StdDevTask(s), MinTask(s), MaxTask(s))
      case BooleanType => Seq(edd.BooleanHistogram(s))
      case TimestampType => Seq(
        edd.TimeMinTask(s),
        edd.TimeMaxTask(s),
        edd.YearHistogram(s),
        edd.MonthHistogram(s),
        edd.DoWHistogram(s),
        edd.HourHistogram(s)
      )
      case StringType => Seq(
        edd.CntTask(s),
        edd.StringMinLenTask(s),
        edd.StringMaxLenTask(s),
        edd.StringDistinctCountTask(s)
      )
    }
  }

}

private[smv] class NullRate(override val df: DataFrame)(colNames: String*) extends EddTaskGroup {
  val listSeq =
    if (colNames.isEmpty)
      df.columns.toSeq
    else
      colNames.toSet.toSeq

  override val taskList = listSeq.map{ l =>
    NullRateTask(df(l))
  }
}

private[smv] class EddHistogram(override val df: DataFrame)(histCols: HistColumn*) extends EddTaskGroup {
  private def createHist(histCol: edd.Hist) = {
    val s = df(histCol.colName)
    df.schema(histCol.colName).dataType match {
      case StringType =>
        if (histCol.sortByFreq) edd.StringByFreqHistogram(s)
        else edd.StringByKeyHistogram(s)
      case _: NumericType => edd.BinNumericHistogram(s, histCol.binSize)
      case BooleanType => edd.BooleanHistogram(s)
      case t => throw new IllegalArgumentException(s"data type: ${t} is not supported")
    }
  }

  override val taskList = histCols.map{ c =>
    c match {
      case h: edd.Hist => createHist(h)
      case edd.AmtHist(colName: String) => edd.AmountHistogram(df(colName))
      case t => throw new IllegalArgumentException(s"data type: ${t} is not supported")
    }
  }
}
