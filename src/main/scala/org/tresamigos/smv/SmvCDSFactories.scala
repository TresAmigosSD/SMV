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

import org.apache.spark.sql.Column
import org.apache.spark.sql.catalyst.dsl.expressions._

object TopNRecs {
  def apply(maxElems: Int, orderCol: Column, others: Column*): FilterCDS = {
    new SmvTopNRecsCDS(maxElems, (orderCol +: others).map{o => o.toExpr})
  }
}

object IntInLastN {
  def apply (t: String, n: Int) = {
    val condition = ($"$t" >= $"_$t" && $"$t" < ($"_$t" + n)) 
    SmvSelfCompareCDS(condition)
  }
}

object TillNow {
  def apply (t: String) = {
    SmvSelfCompareCDS($"$t" >= $"_$t")
  }
}

/**
 * TODO:
 *   - TimeInLastNDays/Months/Weeks/Quarters/Years
 *   - PanelInLastNDays/Months/Weeks/Quarters/Years
 *   - InLastNRec/InFirstNRec as SelfCompareCDS 
 *       SmvTopNRecsCDS from TillNow
 **/