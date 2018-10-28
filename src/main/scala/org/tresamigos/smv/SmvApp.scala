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


import org.apache.log4j.LogManager
import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkContext


/**
 * Driver for SMV applications.  Most apps do not need to override this class and should just be
 * launched using the SmvApp object (defined below)
 */
class SmvApp(_spark: SparkSession) {
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
}

/**
 * Common entry point for all SMV applications.  This is the object that should be provided to spark-submit.
 */
object SmvApp {
  var app: SmvApp = _

  def init(_spark: SparkSession) = {
    app = new SmvApp(_spark)
    app
  }
}
