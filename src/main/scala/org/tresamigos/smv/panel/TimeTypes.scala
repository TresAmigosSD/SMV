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

case class Year(year: Int) extends Ordered[Year] {
  def compare(that: Year) = this.year - that.year
}


case class Month(year: Int, month: Int) extends Ordered[Month] {
  val month70 = (year - 1970) * 12 + month
  def compare(that: Month) = this.month70 - that.month70
}

case class Day(year: Int, month: Int, day: Int) extends Ordered[Day] {
  private val MILLIS_PER_DAY = 86400000
  val day70: Long = new LocalDate(year, month, day).
         toDateTimeAtStartOfDay(DateTimeZone.UTC).
         getMillis / MILLIS_PER_DAY

  def compare(that: Day) = (this.day70 - that.day70).signum
}
