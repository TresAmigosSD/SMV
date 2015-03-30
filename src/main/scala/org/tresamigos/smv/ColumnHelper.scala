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

import java.util.Calendar
import java.sql.Timestamp
import com.rockymadden.stringmetric.phonetic.SoundexAlgorithm

class ColumnHelper(column: Column) {
  
  private val expr = extractExpr(column)
  
  /** convert Column to Expression */
  def toExpr = extractExpr(column)
  
  /** NullSub 
   *  Should consider use coalesce(c1, c2) function going forward
   */
  def smvNullSub[T](newv: T) = {
    val name = s"NullSub($column,$newv)"
    new Column(Alias(If(IsNull(expr), Literal(newv), expr), name)())
  }
  
  def smvNullSub(that: Column) = {
    val name = s"NullSub($column,$that)"
    new Column(Alias(If(IsNull(expr), extractExpr(that), expr), name)())
  }
  
  /** LEFT(5) should be replaced by substr(0,5) */
  
  /** LENGTH */ 
  def smvLength = {
    val name = s"SmvLength($column)"
    val f = (s:Any) => if(s == null) null else s.asInstanceOf[String].size
    new Column(Alias(ScalaUdf(f, IntegerType, Seq(expr)), name)())
  }
  
  /** SmvStrToTimestamp */
  def smvStrToTimestamp(fmt: String) = {
    val name = s"SmvStrToTimestamp($column,$fmt)"
    val fmtObj = new java.text.SimpleDateFormat(fmt)
    val f = (s:Any) => 
      if(s == null) null 
      else new Timestamp(fmtObj.parse(s.asInstanceOf[String]).getTime())
    new Column(Alias(ScalaUdf(f, TimestampType, Seq(expr)), name)())
  }
  
  /** SmvYear */
  def smvYear = {
    val name = s"SmvYear($column)"
    val cal : Calendar = Calendar.getInstance()
    val f = (ts:Any) => 
      if(ts == null) null 
      else {
        cal.setTimeInMillis(ts.asInstanceOf[Timestamp].getTime())
        cal.get(Calendar.YEAR)
      }
      
    new Column(Alias(ScalaUdf(f, IntegerType, Seq(expr)), name)() )
  }
  
  /** SmvMonth */
  def smvMonth = {
    val name = s"SmvMonth($column)"
    val cal : Calendar = Calendar.getInstance()
    val f = (ts:Any) => 
      if(ts == null) null 
      else {
        cal.setTimeInMillis(ts.asInstanceOf[Timestamp].getTime())
        cal.get(Calendar.MONTH) + 1
      }
      
    new Column(Alias(ScalaUdf(f, IntegerType, Seq(expr)), name)() )
  }
  
  /** SmvQuarter */
  def smvQuarter = {
    val name = s"SmvQuarter($column)"
    val cal : Calendar = Calendar.getInstance()
    val f = (ts:Any) => 
      if(ts == null) null 
      else {
        cal.setTimeInMillis(ts.asInstanceOf[Timestamp].getTime())
        cal.get(Calendar.MONTH)/3 + 1
      }
      
    new Column(Alias(ScalaUdf(f, IntegerType, Seq(expr)), name)() )
  }
   
  /** SmvDayOfMonth */
  def smvDayOfMonth = {
    val name = s"SmvDayOfMonth($column)"
    val cal : Calendar = Calendar.getInstance()
    val f = (ts:Any) => 
      if(ts == null) null 
      else {
        cal.setTimeInMillis(ts.asInstanceOf[Timestamp].getTime())
        cal.get(Calendar.DAY_OF_MONTH)
      }
      
    new Column(Alias(ScalaUdf(f, IntegerType, Seq(expr)), name)() )
  }
    
  /** SmvDayOfWeek */
  def smvDayOfWeek = {
    val name = s"SmvDayOfWeek($column)"
    val cal : Calendar = Calendar.getInstance()
    val f = (ts:Any) => 
      if(ts == null) null 
      else {
        cal.setTimeInMillis(ts.asInstanceOf[Timestamp].getTime())
        cal.get(Calendar.DAY_OF_WEEK)
      }
      
    new Column(Alias(ScalaUdf(f, IntegerType, Seq(expr)), name)() )
  }
  
  /** SmvHour */
  def smvHour = {
    val name = s"SmvHour($column)"
    val fmtObj=new java.text.SimpleDateFormat("HH")
    val f = (ts:Any) => 
      if(ts == null) null 
      else fmtObj.format(ts.asInstanceOf[Timestamp]).toInt
      
    new Column(Alias(ScalaUdf(f, IntegerType, Seq(expr)), name)() )
  }
   
  /** AmountBin */
  def smvAmtBin = {
    val name = s"SmvAmtBin($column)"
    val f = (rawv:Any) => 
      if(rawv == null) null 
      else {
        val v = rawv.asInstanceOf[Double]
        if (v < 0.0)
          math.floor(v/1000)*1000
        else if (v == 0.0)
          0.0
        else if (v < 10.0)
          0.01
        else if (v < 200.0)
          math.floor(v/10)*10
        else if (v < 1000.0)
          math.floor(v/50)*50
        else if (v < 10000.0)
          math.floor(v/500)*500
        else if (v < 1000000.0)
          math.floor(v/5000)*5000
        else 
          math.floor(v/1000000)*1000000
      }
      
    new Column(Alias(ScalaUdf(f, DoubleType, Seq(expr)), name)() )
  }
  
  /** NumericBin */
  def smvNumericBin(min: Double, max: Double, n: Int) = {
    val name = s"SmvNumericBin($column,$min,$max,$n)"
    val delta = (max - min) / n
    val f = (rawv:Any) => 
      if(rawv == null) null 
      else {
        val v = rawv.asInstanceOf[Double]
        if (v == max) min + delta * (n - 1)
        else min + math.floor((v - min) / delta) * delta
      }
    
    new Column(Alias(ScalaUdf(f, DoubleType, Seq(expr)), name)() )
  }
  
  /** BinFLoor */
  def smvCoarseGrain(bin: Double) = {
    val name = s"SmvCoarseGrain($column,$bin)"
    val f = (v:Any) => 
      if(v == null) null 
      else math.floor(v.asInstanceOf[Double] / bin) * bin
    new Column(Alias(ScalaUdf(f, DoubleType, Seq(expr)), name)() )
  }
  
  /** SmvSoundex */
  def smvSoundex = {
    val name = s"SmvSoundex($column)"
    val f = (s:Any) => 
      if(s == null) null 
      else SoundexAlgorithm.compute(s.asInstanceOf[String].replaceAll("""[^a-zA-Z]""", "")).getOrElse(null)
    new Column(Alias(ScalaUdf(f, DoubleType, Seq(expr)), name)() )
  }
  
  /** SmvStrCat will be defined as a function */
  /** SmvAsArray will be defiend as a function */
}