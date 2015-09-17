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

import org.apache.spark.SparkContext
import scala.collection.mutable.MutableList

private[smv] class RejectLogger(sparkContext: SparkContext, val localMax: Int = 10) extends Serializable {
  private val rejectedRecords = sparkContext.accumulableCollection(MutableList[String]())
  private val rejectedRecordCount = sparkContext.accumulator(0)

  val addRejectedLineWithReason: (String, Exception) => Unit = {
    var localCounter = 0
    (r:String, e:Exception) => {
      if (localCounter < localMax) {
        rejectedRecords += s"${e.toString} @RECORD: ${r}"
      }
      localCounter = localCounter + 1
      rejectedRecordCount += 1
      Unit
    }
  }

  def report: (Int, List[String]) = {
    (rejectedRecordCount.value, rejectedRecords.value.toList)
  }

}
