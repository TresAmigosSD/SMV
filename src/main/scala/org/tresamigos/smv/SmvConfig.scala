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

/** Scaffolding: for python side to test passing in configs.
 *  Target interface for future SmvConfig class
 **/
class SmvConfig2(
  val genEdd: Boolean,
  var _mergedProps: java.util.Map[String, String],
  var _dataDirs: java.util.Map[String, String]
){
  def printall() = {
    println(genEdd)
    println(_mergedProps)
    println(_dataDirs)
  }

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

  def sparkSqlProps = mergedProps.filterKeys(k => k.startsWith("spark.sql."))
  val outputDir = dataDirs("outputDir")
  val historyDir = dataDirs("historyDir")
  val publishDir = dataDirs("publishDir")
  val publishVersion = dataDirs("publishVersion")

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

object SmvConfig2 {
  def defaultConf(dataDir: String) = new SmvConfig2(
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