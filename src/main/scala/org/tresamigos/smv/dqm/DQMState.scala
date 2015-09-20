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

package org.tresamigos.smv.dqm

import scala.util.Try
import org.tresamigos.smv.RejectLogger
import org.apache.spark.{SparkContext, Accumulator}

class DQMState(
    @transient sc: SparkContext,
    ruleNames: Seq[String],
    fixNames: Seq[String]
  ) extends Serializable {

  private val recordCounter: Accumulator[Long] = sc.accumulator(0l)
  private val fixCounters: Map[String, Accumulator[Int]] = fixNames.map{n => (n, sc.accumulator(0))}.toMap
  private val ruleLoggers: Map[String, RejectLogger] = ruleNames.map{n => (n, new RejectLogger(sc, 10))}.toMap

  private var concluded: Boolean = false
  private var recordCounterCopy: Long = _
  private var fixCountersCopy: Map[String, Int] = _
  private var ruleLoggersCopy: Map[String, (Int, List[String])] = _

  def addRec(): Unit = {
    recordCounter += 1l
  }

  def addFixRec(name: String): Unit = {
    fixCounters(name) += 1
  }

  def addRuleRec(name: String, log: String): Unit = {
    val err = new DQMRuleError(name)
    ruleLoggers(name).addRejectedLineWithReason(log, err)
  }

  def snapshot(): Unit = {
    if(!concluded){
      concluded = true
      recordCounterCopy = recordCounter.value
      fixCountersCopy = fixCounters.map{case (k, a) => (k, a.value)}.toMap
      ruleLoggersCopy = ruleLoggers.map{case (k, l) => (k, l.report)}.toMap
    }
  }

  def getRecCount(): Long = {
    require(concluded)
    recordCounterCopy
  }

  def getFixCount(name: String): Int = {
    require(concluded)
    fixCountersCopy(name)
  }

  def getRuleCount(name: String): Int = {
    require(concluded)
    ruleLoggersCopy(name)._1
  }

  def getTaskCount(name: String): Int = {
    require(concluded)
    Try(getRuleCount(name)).recoverWith{case e =>
      Try(getFixCount(name))
    }.get
  }

  def getRuleLog(name: String): Seq[String] = {
    require(concluded)
    ruleLoggersCopy(name)._2.toSeq
  }

  def getAllLog(): Seq[String] = {
    require(concluded)
    val rLog = ruleLoggersCopy.flatMap{case (name, (n, log)) =>
      Seq(s"Rule: ${name}, total count: ${n}") ++ log.toSeq
    }.toSeq
    val fLog = fixCountersCopy.map{case (name, n) =>
      s"Fix: ${name}, total count: ${n}"
    }.toSeq
    rLog ++ fLog
  }

  def totalFixCount(): Int = {
    require(concluded)
    fixCountersCopy.values.reduce(_ + _)
  }

  def totalRuleCount(): Int = {
    require(concluded)
    ruleLoggersCopy.values.map{_._1}.reduce(_ + _)
  }
}

class DQMRuleError(ruleName: String) extends Exception(ruleName) with Serializable
