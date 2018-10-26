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


import collection.JavaConverters._
import java.util.List
import scala.collection.mutable
import scala.io.Source
import scala.util.{Try, Success, Failure}

import org.apache.log4j.LogManager
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.{SparkContext, SparkConf}
import org.joda.time._, format._

import org.tresamigos.smv.util.Edd


/**
 * Driver for SMV applications.  Most apps do not need to override this class and should just be
 * launched using the SmvApp object (defined below)
 */
class SmvApp(val smvConfig: SmvConfig, _spark: SparkSession) {
  val log         = LogManager.getLogger("smv")

  lazy val smvVersion  = {
    val smvHome = sys.env("SMV_HOME")
    val versionFile = Source.fromFile(f"${smvHome}/.smv_version")
    val nextLine = versionFile.getLines.next
    versionFile.close
    nextLine
  }

  val sparkSession = _spark 

  val sc         = sparkSession.sparkContext
  val sqlContext = sparkSession.sqlContext

  // Since OldVersionHelper will be used by executors, need to inject the version from the driver
  OldVersionHelper.version = sc.version

}

/**
 * Common entry point for all SMV applications.  This is the object that should be provided to spark-submit.
 */
object SmvApp {
  var app: SmvApp = _

  def init(smvConf: SmvConfig, _spark: SparkSession) = {
    app = new SmvApp(smvConf, _spark)
    app
  }
}
