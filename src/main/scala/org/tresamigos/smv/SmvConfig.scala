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

import scala.util.Try

import java.io.{IOException, InputStreamReader, FileInputStream, File}
import java.util.{Properties, ArrayList}

import scala.collection.JavaConversions._
import java.util.Collections

/** Scala side replicate of python side SmvConfig
 *  Command line parsing and props file reading are process on Python
 *  side. The conf result set is passed to Scala side for Scala functions.
 **/
class SmvConfig(
  val genEdd: Boolean,
  var _mergedProps: java.util.Map[String, String],
  var _dataDirs: java.util.Map[String, String]
){

  /**For dynamic config to reset SmvConfig
   * Since genEdd will only be passed in from command line, not part 
   * of dynamic changes, no need to update
   **/
  def reset(
    _props: java.util.Map[String, String], 
    _data_dirs: java.util.Map[String, String]
  ) {
    _mergedProps = _props
    _dataDirs = _data_dirs
  }

  def mergedProps: Map[String, String] = mapAsScalaMap(_mergedProps).toMap
  def dataDirs: Map[String, String] = mapAsScalaMap(_dataDirs).toMap

  private[smv] def stageNames = { splitProp("smv.stages").toSeq }

  def outputDir = dataDirs("outputDir")
  def historyDir = dataDirs("historyDir")
  def publishDir = dataDirs("publishDir")
  def publishVersion = dataDirs("publishVersion")

  def jdbcUrl: String =
    mergedProps.get("smv.jdbc.url") match {
      case Some(url) =>
        url
      case _ =>
        throw new SmvRuntimeException("JDBC url not specified in SMV config")
    }
  
  def jdbcDriver: String = mergedProps.get("smv.jdbc.driver") match {
    case None => throw new SmvRuntimeException("JDBC driver is not specified in SMV config")
    case Some(ret) => ret
  }

  /**
   * convert the property value to an array of strings.
   * Return empty array if original prop value was an empty string
   */
  private[smv] def splitProp(propName: String) = {
    val propVal = mergedProps.getOrElse(propName, "")
    if (propVal == "")
      Array[String]()
    else
      propVal.split(Array(',', ':')).map(_.trim)
  }
}

object SmvConfig {
  /** Purely for Scala side testing
   *  Some scala tests need an instance of SmvApp, which need a 
   *  SmvConfig. Since Scala test will run without Python, have to
   *  create a default SmvConfig without Python
   **/
  def defaultConf(dataDir: String) = new SmvConfig(
    false,
    Collections.emptyMap(),
    mapAsJavaMap(Map(
      "dataDir" -> dataDir,
      "inputDir" -> (dataDir + "/input"),
      "outputDir" -> (dataDir + "/output"),
      "historyDir" -> (dataDir + "/history"),
      "publishDir" -> (dataDir + "/publish"),
      "publishVersion" -> null
    ))
  )
}