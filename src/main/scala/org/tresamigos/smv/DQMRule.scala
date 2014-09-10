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
  def check(c: Any): Boolean = true
  def fix(c: Any): Any = c
}

case object NoOpRule extends DQMRule 
   
case class BoundRule[T](lower: T, upper: T)(implicit ord: Ordering[T]) extends DQMRule {
  override def check(c: Any): Boolean = {
    ord.lteq(lower, c.asInstanceOf[T]) && ord.lteq(c.asInstanceOf[T], upper) 
  }
}

case class SetRule[T](s: Set[T]) extends DQMRule {
  override def check(c: Any): Boolean = {
    s.contains(c.asInstanceOf[T])
  }
}

//TODO: add reject loging
//TODO: UpperBoundRule, LowerBoundRule, InYearRule, InPastRule, InFutureRule,
//etc.
