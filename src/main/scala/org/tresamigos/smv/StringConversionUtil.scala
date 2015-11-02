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


private[smv] object StringConversionUtil {
  val IntThreshold: Int = Math.ceil(Int.MaxValue / 1.5).toInt
  val FloatThreshold: Float = Float.MaxValue / 1.5f

  def canConvertToInt(str: String) : Boolean =  {
    try {
      str.toInt < IntThreshold
    } catch {
      case _ : Exception => false
    }
  }

  def canConvertToLong(str: String) : Boolean =  {
    try {
      str.toLong
      true
    } catch {
      case _ : Exception => false
    }
  }

  def canConvertToFloat(str: String) : Boolean =  {
    try {
      str.toFloat < FloatThreshold
    } catch {
      case _ : Exception => false
    }
  }

  def canConvertToDouble(str: String) : Boolean =  {
    try {
      str.toDouble
      true
    } catch {
      case _ : Exception => false
    }
  }

  def canConvertToBoolean(str: String) : Boolean =  {
    try {
      str.toBoolean
      true
    } catch {
      case _ : Exception => false
    }
  }

  def canConvertToDate(str: String, fmt: String) : Boolean = {

    try {
      val fmtObj = new java.text.SimpleDateFormat(fmt)
      fmtObj.setLenient(false)
      fmtObj.parse(str)
      true
    } catch {
      case _ : Exception => false
    }
  }
}
