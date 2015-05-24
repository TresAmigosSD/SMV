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

import org.apache.spark.sql.{Column, ColumnName}
import org.apache.spark.sql.catalyst.dsl.expressions._

import org.apache.spark.sql.catalyst.expressions._

/**
 * TopNRecs: Return top N records on a given order 
 * 
 * Eg.
 * TopNRecs(3, $"amount".desc) 
 * 
 * Return the 3 records with largest "amount" field
 **/
object TopNRecs {
  def apply(maxElems: Int, orderCols: Column*): SmvCDS = {
    new SmvTopNRecsCDS(maxElems, orderCols.map{o => o.toExpr})
  }
}

/**
 * TillNow(t): Return records "before" current record based on column $"$t"
 **/
case class TillNow(t: String) extends SmvSelfCompareCDS with RunAggOptimizable {
  val condition = ($"$t" >= $"_$t")
  
  def createRunAggIterator(inSchema: SmvSchema, cum: Seq[AggregateFunction]): (Iterable[Row]) => Iterable[Row] = {
    val timeOrdinal = inSchema.getIndices(t)(0)
    val order = inSchema.findEntry(t).get.asInstanceOf[NativeSchemaEntry].ordering.asInstanceOf[Ordering[Any]]
    implicit object RowOrdering extends Ordering[Row] {
      def compare(a:Row, b:Row) = order.compare(a(timeOrdinal),b(timeOrdinal))
    }
    
    {it: Iterable[Row] =>
      val newcum = cum.map{_.newInstance()}
      it.toSeq.sorted.map{r =>
        newcum.map{_.update(r)}
        val sum = newcum.map{_.eval(null)}
        new GenericRow(Array[Any](r.toSeq ++ sum: _*))
      }
    }
  }
}

/**
 * IntInLastN: Return records within current value of an Int column and (current value - N)
 **/
case class IntInLastN(t: String, n: Int) extends SmvSelfCompareCDS {
  val condition = ($"$t" >= $"_$t" && $"$t" < ($"_$t" + n)) 
}

/**
 * TimeInLastNDays: Return records in last N days according to a timestamp field
 **/
case class TimeInLastNDays(t: String, n: Int) extends SmvSelfCompareCDS {
  val condition = ($"$t" >= $"_$t" && $"$t" < (new ColumnName("_" + t)).smvPlusDays(n).toExpr)
}

/**
 * TimeInLastNMonths: Return records in last N months according to a timestamp field
 **/
case class TimeInLastNMonths(t: String, n: Int) extends SmvSelfCompareCDS {
  val condition = ($"$t" >= $"_$t" && $"$t" < (new ColumnName("_" + t)).smvPlusMonths(n).toExpr)
}

/**
 * TimeInLastNWeeks: Return records in last N weeks according to a timestamp field
 **/
case class TimeInLastNWeeks(t: String, n: Int) extends SmvSelfCompareCDS {
  val condition = ($"$t" >= $"_$t" && $"$t" < (new ColumnName("_" + t)).smvPlusWeeks(n).toExpr)
}

/**
 * TimeInLastNYears: Return records in last N years according to a timestamp field
 **/
case class TimeInLastNYears(t: String, n: Int) extends SmvSelfCompareCDS {
  val condition = ($"$t" >= $"_$t" && $"$t" < (new ColumnName("_" + t)).smvPlusYears(n).toExpr)
}

/**
 * TODO:
 *   - PanelInLastNDays/Months/Weeks/Quarters/Years
 *   - InLastNRec/InFirstNRec as SelfCompareCDS 
 *       SmvTopNRecsCDS from TillNow
 **/