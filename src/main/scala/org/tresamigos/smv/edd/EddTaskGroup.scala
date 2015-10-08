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
    val aggCols = taskList.map{t => t.aggCol}
    val resultCols = taskList.map{t => t.resultCols}

    // this DF only has 1 row
    val res = df.agg(aggCols.head, aggCols.tail: _*).smvCoalesce(1)

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
    val resRdds = resultCols.map{rcols => res.select(rcols: _*).rdd}
    val resRdd = new UnionRDD(df.sqlContext.sparkContext, resRdds).coalesce(1)

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
