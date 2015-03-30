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
import org.apache.spark.sql.contrib.smv._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.types._

class ColumnHelper(column: Column) {
  
  private val expr = extractExpr(column)
  
  /** convert Column to Expression */
  def toExpr = extractExpr(column)
  
  /** NullSub */
  def smvNullSub[T](newv: T) = {
    new Column(If(IsNull(expr), Literal(newv), expr))
  }
  
  def smvNullSub(that: Column) = {
    new Column(If(IsNull(expr), extractExpr(that), expr))
  }
  
  /** LEFT(5) should be replaced by substr(0,5) */
  
  /** LENGTH */ 
  def smvLength = {
    val f = (s:Any) => if(s == null) null else s.asInstanceOf[String].size
    new Column(ScalaUdf(f, IntegerType, Seq(expr)))
  }
  
  /** SmvStrToTimestamp */
  def smvStrToTimestamp(fmt: String) = {
    val fmtObj = new java.text.SimpleDateFormat(fmt)
    val f = (s:Any) => 
      if(s == null) null 
      else new java.sql.Timestamp(fmtObj.parse(s.asInstanceOf[String]).getTime())
    new Column(ScalaUdf(f, TimestampType, Seq(expr)))
  }
}