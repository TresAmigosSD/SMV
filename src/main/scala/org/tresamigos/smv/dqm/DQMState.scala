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
package dqm

import org.apache.spark.{Accumulator, SparkContext}
import org.apache.spark.util.{AccumulatorV2, LongAccumulator}
import org.tresamigos.smv.util.IntAccumulator

import scala.util.Try
import scala.annotation.meta.param
import scala.collection.JavaConverters._
import scala.collection.mutable.MutableList

/**
 * DQMState keeps tracking of [[org.tresamigos.smv.dqm.DQMTask]] behavior on a DF
 *
 * Since the logs are implemented with Aggregators, it need a SparkContext to construct.
 * A list of DQMRule names and a list of DQMFix names are needed also.
 **/
class DQMState(
    @(transient @param) sc: SparkContext,
    ruleNames: Seq[String],
    fixNames: Seq[String]
) extends Serializable {

  private val recordCounter: LongAccumulator = sc.longAccumulator
  private val parserLogger                   = new RejectLogger(sc, 10, "parser")
  private val fixCounters: Map[String, IntAccumulator] = fixNames.map{ n =>
    val acc = new IntAccumulator; sc.register(acc); (n, acc)
  }.toMap
  private val ruleLoggers: Map[String, RejectLogger] = ruleNames.map { n =>
    (n, new RejectLogger(sc, 10, n))
  }.toMap
  private var miscLog: Seq[String] = Seq.empty

  private var concluded: Boolean                                = false
  private var recordCounterCopy: Long                           = _
  private var parserLoggerCopy: (Int, List[String])             = _
  private var fixCountersCopy: Map[String, Int]                 = _
  private var ruleLoggersCopy: Map[String, (Int, List[String])] = _

  /** add one on the overall record counter */
  private[smv] def addRec(): Unit = {
    recordCounter add 1l
  }

  /** add one record to the log of parser errors. */
  private[smv] def addParserErrorRec(log: String): Unit = {
    parserLogger.add(log)
  }

  /** add one on the "fix" counter for the given fix name */
  private[smv] def addFixRec(name: String): Unit = {
    fixCounters(name) add 1
  }

  /** add one on the "rule" counter for the give rule name, and also log the referred
   *  columns of example records which failed the rule **/
  private[smv] def addRuleRec(name: String, log: String): Unit = {
    val err = new DQMRuleError(name)
    ruleLoggers(name).add(s"${err} @FIELDS: ${log}")
  }

  private[smv] def addMiscLog(entry: String) =
    miscLog = miscLog :+ entry

  /**
   * take a snapshot of the counters of loggers and create a local copy
   *
   * Since we might want to refer this DQMState multiple times, while the accumulators
   * might keep updating, so we need to take a snapshot before we use the results in
   * the DQMState
   **/
  private[smv] def snapshot(): Unit = {

    /** snapshot need to run once and only once */
    if (!concluded) {
      concluded = true
      recordCounterCopy = recordCounter.value
      // TODO: rename parser to parserErrors
      parserLoggerCopy = parserLogger.report
      fixCountersCopy = fixCounters.map { case (k, a) => (k, a.value) }.toMap
      ruleLoggersCopy = ruleLoggers.map { case (k, l) => (k, l.report) }.toMap
    }
  }

  /** get the overall record count */
  def getRecCount(): Long = {
    require(concluded)
    recordCounterCopy
  }

  /** get the total parser fail count */
  def getParserCount(): Int = {
    require(concluded)
    parserLoggerCopy._1
  }

  /** for the fix with the given name, return the time it is triggered */
  def getFixCount(name: String): Int = {
    require(concluded)
    fixCountersCopy(name)
  }

  /** for the rule with the given name, return the time it is triggered */
  def getRuleCount(name: String): Int = {
    require(concluded)
    ruleLoggersCopy(name)._1
  }

  /** for the rule or fix with the given name, return the time it is triggered */
  def getTaskCount(name: String): Int = {
    require(concluded)
    /* try whether we can find the task in the list of rules, then try on the list of fixes */
    Try(getRuleCount(name)).recoverWith {
      case e =>
        Try(getFixCount(name))
    }.get
  }

  /** get the example parser fails */
  def getParserLog(): Seq[String] = {
    require(concluded)
    parserLoggerCopy._2.toSeq
  }

  /** for the rule with the given name, return the example failed records */
  def getRuleLog(name: String): Seq[String] = {
    require(concluded)
    ruleLoggersCopy(name)._2.toSeq
  }

  /** return all the example failed records from all the rules */
  def getAllLog(): Seq[String] = {
    require(concluded)
    val rLog = ruleLoggersCopy.flatMap {
      case (name, (n, log)) =>
        Seq(s"Rule: ${name}, total count: ${n}") ++ log.toSeq
    }.toSeq
    val fLog = fixCountersCopy.map {
      case (name, n) =>
        s"Fix: ${name}, total count: ${n}"
    }.toSeq
    val pLog = getParserLog()
    rLog ++ fLog ++ pLog ++ miscLog
  }

  /** return the total number of times fixes get triggered */
  def getTotalFixCount(): Int = {
    require(concluded)
    fixCountersCopy.values.reduce(_ + _)
  }

  /** return the total number of times rules get triggered */
  def getTotalRuleCount(): Int = {
    require(concluded)
    ruleLoggersCopy.values.map { _._1 }.reduce(_ + _)
  }
}

class DQMRuleError(ruleName: String) extends Exception(ruleName) with Serializable

/**
 * One instance of this RejectLogger will be created per DQMState.
 * This class keeps track of how many errors were logged and only keeps the
 * first N (default to 10) records to avoid overwhelming the log files.
 */
private[smv] class RejectLogger(sparkContext: SparkContext,
                                val localMax: Int = 10,
                                loggerName: String)
    extends Serializable {
  private val rejectedRecords = sparkContext.collectionAccumulator[String]
  private val rejectedRecordCount = {
    val acc = new IntAccumulator; sparkContext.register(acc, loggerName); acc
  }

  /**
   * adds an error message to the log.  Note that there will be a separate
   * localCounter per schedules spark task.
   */
  val add: (String) => Unit = {
    var localCounter = 0
    (r: String) =>
      {
        if (localCounter < localMax) {
          rejectedRecords add r
        }
        localCounter = localCounter + 1
        rejectedRecordCount add 1
        Unit
      }
  }

  /**
   * Generate the reject logger report which consists of a tuple of
   * (total count of errors, first N log errors)
   */
  def report : (Int, List[String]) = {
    (rejectedRecordCount.value, rejectedRecords.value.asScala.toList)
  }
}
