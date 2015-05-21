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
object TillNow {
  def apply (t: String) = {
    SmvSelfCompareCDS($"$t" >= $"_$t")
  }
}

/**
 * IntInLastN: Return records within current value of an Int column and (current value - N)
 **/
object IntInLastN {
  def apply (t: String, n: Int) = {
    val condition = ($"$t" >= $"_$t" && $"$t" < ($"_$t" + n)) 
    SmvSelfCompareCDS(condition)
  }
}

/**
 * TimeInLastNDays: Return records in last N days according to a timestamp field
 **/
object TimeInLastNDays {
  def apply (t: String, n: Int) = {
    SmvSelfCompareCDS($"$t" >= $"_$t" && $"$t" < (new ColumnName("_" + t)).smvPlusDays(n).toExpr)
  }
}

/**
 * TimeInLastNMonths: Return records in last N months according to a timestamp field
 **/
object TimeInLastNMonths {
  def apply (t: String, n: Int) = {
    SmvSelfCompareCDS($"$t" >= $"_$t" && $"$t" < (new ColumnName("_" + t)).smvPlusMonths(n).toExpr)
  }
}

/**
 * TimeInLastNWeeks: Return records in last N weeks according to a timestamp field
 **/
object TimeInLastNWeeks {
  def apply (t: String, n: Int) = {
    SmvSelfCompareCDS($"$t" >= $"_$t" && $"$t" < (new ColumnName("_" + t)).smvPlusWeeks(n).toExpr)
  }
}

/**
 * TimeInLastNYears: Return records in last N years according to a timestamp field
 **/
object TimeInLastNYears {
  def apply (t: String, n: Int) = {
    SmvSelfCompareCDS($"$t" >= $"_$t" && $"$t" < (new ColumnName("_" + t)).smvPlusYears(n).toExpr)
  }
}

/**
 * TODO:
 *   - PanelInLastNDays/Months/Weeks/Quarters/Years
 *   - InLastNRec/InFirstNRec as SelfCompareCDS 
 *       SmvTopNRecsCDS from TillNow
 **/