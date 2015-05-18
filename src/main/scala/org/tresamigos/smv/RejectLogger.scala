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

abstract class RejectLogger extends Serializable {
  val addRejectedLineWithReason: (String, Exception) => Unit = (r:String, e:Exception) => Unit
  val addRejectedSeqWithReason: (Seq[Any], Exception) => Unit = (r:Seq[Any], e:Exception) => Unit
  def rejectedReport: List[(String, String)] = Nil
}

object RejectLogger {
  implicit val rejectLogger = TerminateRejectLogger;
}

object NoOpRejectLogger extends RejectLogger 

object TerminateRejectLogger extends RejectLogger {
  override val addRejectedLineWithReason: (String, Exception) => Unit = (r:String, e:Exception) => {
    throw e
    Unit
  }
  override val addRejectedSeqWithReason: (Seq[Any], Exception) => Unit = (r:Seq[Any], e:Exception) => {
    throw e
    Unit
  }
}

class SCRejectLogger(sparkContext: SparkContext, val localMax: Int = 10) extends RejectLogger {
  private val rejectedRecords = sparkContext.accumulableCollection(MutableList[(String, String)]())
  private val rejectedRecordCount = sparkContext.accumulator(0)

  override val addRejectedLineWithReason: (String, Exception) => Unit = {
    var localCounter = 0
    (r:String, e:Exception) => {
      if (localCounter < localMax) {
        rejectedRecords += ((r, e.toString))
      }
      localCounter = localCounter + 1
      rejectedRecordCount += 1
      Unit
    }
  }
 
  override val addRejectedSeqWithReason: (Seq[Any], Exception) => Unit = 
    (r:Seq[Any], e:Exception) => addRejectedLineWithReason(r.mkString(","), e)


  override def rejectedReport: List[(String, String)] = {
    if (rejectedRecordCount.value > 0) {
      val rep = rejectedRecords.value
      if (rejectedRecordCount.value > rep.size){
        (rep :+ ((s"More rejects!! Total rejected records: $rejectedRecordCount",""))).toList
      } else {
        (rep :+ ((s"Total rejected records: $rejectedRecordCount",""))).toList
      }
    } else {
      Nil
    }
  }

}
