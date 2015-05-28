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

package org.tresamigos.smv.panel

import org.joda.time._

trait PartialTime extends Ordered[PartialTime] with Serializable {
  def getValue(): Int
  def compare(that: PartialTime) = (this.getValue() - that.getValue()).signum
}

case class Month(year: Int, month: Int) extends PartialTime {
  val month70: Int = (year - 1970) * 12 + month
  def getValue() = month70
}

case class Day(year: Int, month: Int, day: Int) extends PartialTime {
  private val MILLIS_PER_DAY = 86400000
  val day70: Int = (new LocalDate(year, month, day).
         toDateTimeAtStartOfDay(DateTimeZone.UTC).
         getMillis / MILLIS_PER_DAY).toInt
  def getValue() = day70
}

abstract class Panel extends Serializable {
  val start: PartialTime
  val end: PartialTime
  val startValue = start.getValue()
  val endValue = end.getValue()
  def hasInRange(t: PartialTime) = (start <= t && t <= end)
  def hasInRange(t: Int) = (startValue <= t && t <= endValue)

  def createValues(): Iterable[Int]
}

abstract class ContinuousPanel extends Panel {
  def createValues(): Iterable[Int] = {
    startValue + 1 until endValue
  }
}

case class MonthlyPanel(start: Month, end: Month) extends ContinuousPanel
case class DailyPanel(start: Day, end: Day) extends ContinuousPanel
