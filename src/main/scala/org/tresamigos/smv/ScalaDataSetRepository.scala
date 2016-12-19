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

import org.apache.spark.sql.DataFrame
import dqm.{SmvDQM, DQMValidator}
import scala.collection.JavaConversions._
import scala.util.{Success, Failure}

/**
 * Repository for data sets written in Scala.
 */
class ScalaDataSetRepository extends SmvDataSetRepository {
  private var scalaDataSets: Map[String, SmvDataSet] = Map.empty

  private[smv] def dsForName(modUrn: String): Option[SmvDataSet] =
    scalaDataSets.get(modUrn) orElse {
      val ret =
        if (isExternalMod(modUrn))
          Some(SmvExtDataSet(urn2fqn(modUrn)))
        else
          SmvReflection.findObjectByName[SmvDataSet](urn2fqn(modUrn)).toOption

      if (ret.isDefined)
        scalaDataSets = scalaDataSets + (modUrn -> ret.get)
      ret
    }

  override def hasDataSet(modUrn: String): Boolean =
    dsForName(modUrn).isDefined

  override def isEphemeral(modUrn: String): Boolean =
    dsForName(modUrn) match {
      case None => notFound(modUrn, "cannot check if the module is ephemeral")
      case Some(ds) => ds.isEphemeral
    }

  override def getDqm(modUrn: String): SmvDQM =
    dsForName(modUrn) match {
      case None => notFound(modUrn, "cannot get dqm")
      case Some(ds) => ds.createDsDqm()
    }

  private def notFound(modUrn: String, msg: String) =
    throw new SmvRuntimeException(s"dataset [${modUrn}] is not found in ${getClass.getName}: ${msg}")

  override def outputModsForStage(stageName: String): String = {
    val stage = SmvApp.app.smvConfig.stages.findStage(stageName)
    stage.allOutputModules.map(_.fqn).mkString(",")
  }

  override def dependencies(modUrn: String): String =
    dsForName(modUrn) match {
      case None => notFound(modUrn, "cannot get dependencies")
      case Some(ds) => ds.requiresDS.map(_.urn).mkString(",")
    }

  override def getDataFrame(modUrn: String, validator: DQMValidator,
    known: java.util.Map[String, DataFrame]): DataFrame =
    dsForName(modUrn) match {
      case None => notFound(modUrn, "cannot get dataframe")
      case Some(ds) => ds.doRun(validator, known)
    }

  override def rerun(modUrn: String, validator: DQMValidator,
    known: java.util.Map[String, DataFrame]): DataFrame = {
    val parentClassLoader = getClass.getClassLoader
    val cl = class_loader.SmvClassLoader(SmvApp.app.smvConfig, parentClassLoader)
    val ds = new SmvReflection(cl).objectNameToInstance[SmvDataSet](modUrn)
    val message = shell.ShellCmd.hotdeployIfCapable(ds, parentClassLoader)
    println(message)
    scalaDataSets = scalaDataSets + (modUrn -> ds)
    ds.doRun(validator, known)
  }

  override def datasetHash(modUrn: String, includeSuperClass: Boolean = true): Int =
    dsForName(modUrn) match {
      case None => notFound(modUrn, "cannot calc dataset hash")
      case Some(ds) => ds.datasetCRC.toInt
    }
}
