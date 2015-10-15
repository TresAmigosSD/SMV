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

package org.tresamigos.smv.cds

import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.dsl.expressions._
import org.apache.spark.sql.catalyst.expressions._
import org.tresamigos.smv._

/**
 * Return top N records in a given order
 *
 * {{{
 * TopNRecs(3, $"amount".desc)
 * }}}
 * Return the 3 records with largest "amount" field
 **/
object TopNRecs {
  def apply(maxElems: Int, orderCols: Column*): SmvCDS = {
    new SmvTopNRecsCDS(maxElems, orderCols.map{o => o.toExpr})
  }
}

private[smv] case class InLastNWithNull(n: Int) extends SmvCDS {
  def filter(input: CDSSubGroup) = {
    val outIt = {
      val rows = input.crossRows.toSeq.takeRight(n)
      val resSize = rows.size
      if (resSize < n) {
        val nullArr:Array[Any] = new Array(input.crossSchema.fields.size)
        Range(0, n - resSize).map{i => new GenericInternalRow(nullArr)} ++ rows
      } else rows
    }
    CDSSubGroup(input.currentSchema, input.crossSchema, input.currentRow, outIt)
  }
}

/**
 * Return records "before" current record based on column timeColName
 **/
case class Before(timeColName: String) extends SmvSelfCompareCDS {
  val condition = ($"$timeColName" > $"_$timeColName")
}

/**
 * For a given integer type column, `intColName`, return records
 * within `current value` and `(current value - n)``
 **/
case class IntInLastN(intColName: String, n: Int) extends SmvSelfCompareCDS {
  val condition = ($"$intColName" >= $"_$intColName" && $"$intColName" < ($"_$intColName" + n))
}

/**
 * For a given timeStamp type column, `timeColName`, return records in last n days
 **/
case class TimeInLastNDays(timeColName: String, n: Int) extends SmvSelfCompareCDS {
  val condition = ($"$timeColName" >= $"_$timeColName" && $"$timeColName" < (new ColumnName("_" + timeColName)).smvPlusDays(n).toExpr)
}

/**
 * For a given timeStamp type column, `timeColName`, return records in last n months
 **/
case class TimeInLastNMonths(timeColName: String, n: Int) extends SmvSelfCompareCDS {
  val condition = ($"$timeColName" >= $"_$timeColName" && $"$timeColName" < (new ColumnName("_" + timeColName)).smvPlusMonths(n).toExpr)
}

/**
 * For a given timeStamp type column, `timeColName`, return records in last n weeks
 **/
case class TimeInLastNWeeks(timeColName: String, n: Int) extends SmvSelfCompareCDS {
  val condition = ($"$timeColName" >= $"_$timeColName" && $"$timeColName" < (new ColumnName("_" + timeColName)).smvPlusWeeks(n).toExpr)
}

/**
 * For a given timeStamp type column, `timeColName`, return records in last n years
 **/
case class TimeInLastNYears(timeColName: String, n: Int) extends SmvSelfCompareCDS {
  val condition = ($"$timeColName" >= $"_$timeColName" && $"$timeColName" < (new ColumnName("_" + timeColName)).smvPlusYears(n).toExpr)
}

/**
 * TODO:
 *   - PanelInLastNDays/Months/Weeks/Quarters/Years
 *   - InLastNRec/InFirstNRec as SelfCompareCDS
 *       SmvTopNRecsCDS from TillNow
 **/
