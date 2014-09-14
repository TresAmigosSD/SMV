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


// TODO: missing doc.  What does FR stand for?
abstract class DQMRule extends Serializable {
  def symbol: Symbol
  def check(c: Any): Boolean = true
  val fix: Any => Any = {c => c}
}

case class NoOpRule(symbol: Symbol) extends DQMRule 
   
case class BoundRule[T:Ordering](symbol: Symbol, lower: T, upper: T) extends DQMRule {

  private val ord = implicitly[Ordering[T]]

  override def check(c: Any): Boolean = {
    ord.lteq(lower, c.asInstanceOf[T]) && ord.lteq(c.asInstanceOf[T], upper) 
  }

  override val fix: Any => Any = { c =>
    if (ord.lteq(c.asInstanceOf[T], lower)) lower
    else if (ord.lteq(upper, c.asInstanceOf[T])) upper
    else c
  }
}

case class SetRule[T](symbol: Symbol, s: Set[T]) extends DQMRule {
  override def check(c: Any): Boolean = {
    s.contains(c.asInstanceOf[T])
  }
}

//TODO: add reject loging
//TODO: UpperBoundRule, LowerBoundRule, InYearRule, InPastRule, InFutureRule,
//etc.
