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

// Not used in SMV. Consider to remove it from SMV
class OptionNumber[T:Numeric](l:Option[T]) extends Ordered[Option[T]]{
  val n=implicitly[Numeric[T]]
  def +(r:Option[T]) = (l,r) match {
    case (None, None) => None
    case (None, _)    => r
    case (_, None)    => l
    case _            => Some(n.plus(l.get,r.get))
  }
  def unary_-():Option[T] = if (l==None) None else Some(n.negate(l.get))
  def -(r:Option[T]) = (l,r) match {
    case (None, None) => None
    case (None, _)    => - new OptionNumber(r)
    case (_, None)    => l
    case _            => Some(n.minus(l.get,r.get))
  }
  def *(r:Option[T]) = if (l==None || r== None) None else Some(n.times(l.get,r.get))
  def compare(r:Option[T]):Int = (l,r) match {
    case (None, None) => 0
    case (None, _)    => -1
    case (_, None)    => 1
    case _            => n.compare(l.get,r.get)
  }
  def /(r:Option[T]) = {
    n match {
      case num: Fractional[_] => if (l==None || r==None) None else Some(num.div(l.get,r.get))
      case num: Integral[_] => if (l==None || r==None) None else Some(num.quot(l.get,r.get))
    }
  }
}


/*
  implicit def makeOptionNumber[T:Numeric](l: Option[T]) = new OptionNumber(l)

  val a:Option[Double]=Option(1.5)
  val b:Option[Double]=None
  println("a=%s".format(a))
  println("b=%s".format(b))
  println("a+b= %s".format(a+b))
  println("a-b= %s".format(a-b))
  println("a*b= %s".format(a*b))
  println("a/b= %s".format(a/b))
    
*/
