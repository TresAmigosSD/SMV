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
package util

private[smv] object StringConversion {
  val IntThreshold: Int     = Math.ceil(Int.MaxValue / 1.5).toInt
  val FloatThreshold: Float = Float.MaxValue / 1.5f

  def canConvertToInt(str: String): Boolean = {
    try {
      str.toInt < IntThreshold
    } catch {
      case _: Exception => false
    }
  }

  def canConvertToLong(str: String): Boolean = {
    try {
      str.toLong
      true
    } catch {
      case _: Exception => false
    }
  }

  def canConvertToFloat(str: String): Boolean = {
    try {
      str.toFloat < FloatThreshold
    } catch {
      case _: Exception => false
    }
  }

  def canConvertToDouble(str: String): Boolean = {
    try {
      str.toDouble
      true
    } catch {
      case _: Exception => false
    }
  }

  def canConvertToBoolean(str: String): Boolean = {
    try {
      str.toBoolean
      true
    } catch {
      case _: Exception => false
    }
  }

  def canConvertToDate(str: String, fmt: String): Boolean = {

    try {
      val fmtObj = new java.text.SimpleDateFormat(fmt)
      val pos = new java.text.ParsePosition(0)
      fmtObj.setLenient(false)
      fmtObj.parse(str, pos)
      // Since "2012-12-01 12:05:01" can be parsed by "yyyy-MM-dd", need to check whether there are leftovers 
      pos.getIndex == str.length()
    } catch {
      case _: Exception => false
    }
  }

  /** Apply logic to discover Timestamp and Date format from a String 
    @param preType previous discoved type
    @param str current data string

    For the first record, null was passed in as preType. In that case the str is tested
    against supported time format and date format sequencially. Return the first match or
    when no match, default to StringType

    For non-first recore, previous Type was used to test against the string, if passed
    keep the preType, else default to StringType
  */
  def convertToSupportedDateTime(preType: TypeFormat, str: String): TypeFormat = {
    val supportedTime = Seq(
      "MM/dd/yyyy HH:mm:ss",
      "MM/dd/yyyy HH:mm:ss.S",
      "MM-dd-yyyy HH:mm:ss",
      "MM-dd-yyyy HH:mm:ss.S",
      "MMM/dd/yyyy HH:mm:ss",
      "MMM/dd/yyyy HH:mm:ss.S",
      "MMM-dd-yyyy HH:mm:ss",
      "MMM-dd-yyyy HH:mm:ss.S",
      "dd-MMM-yyyy HH:mm:ss",
      "dd-MMM-yyyy HH:mm:ss.S",
      "ddMMMyyyy HH:mm:ss",
      "ddMMMyyyy HH:mm:ss.S",
      "yyyy-MM-dd HH:mm:ss",
      "yyyy-MM-dd HH:mm:ss.S"
    )

    val supportedDate = Seq(
      "MM/dd/yyyy",
      "MM-dd-yyyy",
      "MMM/dd/yyyy",
      "MMM-dd-yyyy",
      "dd-MMM-yyyy",
      "ddMMMyyyy",
      "yyyy-MM-dd"
    )

    if (preType == null){
      val tmType = supportedTime.find(fmt => 
        canConvertToDate(str, fmt)
      ).map(fmt => TimestampTypeFormat(fmt))

      val dtType = supportedDate.find(fmt =>
        canConvertToDate(str, fmt)
      ).map(fmt => DateTypeFormat(fmt))

      tmType.orElse(dtType).getOrElse(StringTypeFormat())
    } else {
      preType match {
        case DateTypeFormat(preFmt) if canConvertToDate(str, preFmt) => preType
        case TimestampTypeFormat(preFmt) if canConvertToDate(str, preFmt) => preType
        case _ => StringTypeFormat()
      }
    }
  }
}
