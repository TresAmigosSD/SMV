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
import org.apache.spark.SparkContext._
import scala.collection.mutable.MutableList

abstract class RejectLogger extends Serializable {
  def addRejectedLineWithReason(r:String, e:Exception): Unit
  def rejectReport(rejectFile: String): Unit
}

class NoOpRejectLogger extends RejectLogger {
  def addRejectedLineWithReason(r:String, e:Exception): Unit = Unit
  def rejectReport(rejectFile: String): Unit = Unit
}

object RejectLogger {
  implicit val rejectLogger = new NoOpRejectLogger;
}

class SCRejectLogger(sparkContext: SparkContext) extends RejectLogger {
  private val rejectedRecords = sparkContext.accumulableCollection(MutableList[(String,String)]())
  private val rejectedRecordCount = sparkContext.accumulator(0)

  def addRejectedLineWithReason(r:String, e:Exception) {
    rejectedRecords += ((r, e.toString))
    rejectedRecordCount += 1
  }

  def rejectReport(rejectFile: String = ""): Unit = {
    if (rejectedRecordCount.value > 0) {
      if (rejectedRecordCount.value > rejectedRecords.value.size){
        rejectedRecords += ((s"More rejects!! Total rejected records: $rejectedRecordCount",""))
      } else {
        rejectedRecords += ((s"Total rejected records: $rejectedRecordCount",""))
      }

      if (rejectFile.isEmpty){
        println(rejectedRecords.value.mkString("\n"))
      }else{
        sparkContext.parallelize(rejectedRecords.value).saveAsTextFile(rejectFile)
      }
    }
  }

}


