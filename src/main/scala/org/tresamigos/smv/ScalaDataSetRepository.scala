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

  private def dsForName(modfqn: String): Option[SmvDataSet] =
    scalaDataSets.get(modfqn) orElse {
      val ret =
        if (isExternal(modfqn))
          Some(SmvExtDataSet(modfqn.substring(ExtDsPrefix.length)))
        else
          SmvReflection.findObjectByName[SmvDataSet](modfqn).toOption

      if (ret.isDefined)
        scalaDataSets = scalaDataSets + (modfqn -> ret.get)
      ret
    }

  override def hasDataSet(modfqn: String): Boolean =
    dsForName(modfqn).isDefined

  override def isExternal(modfqn: String): Boolean =
    modfqn.startsWith(ExtDsPrefix)

  override def isEphemeral(modfqn: String): Boolean =
    if (isExternal(modfqn))
      throw new IllegalArgumentException(s"Cannot know if ${modfqn} is ephemeral because it is external")
    else
      dsForName(modfqn) match {
        case None => notFound(modfqn, "cannot check if the module is ephemeral")
        case Some(ds) => ds.isEphemeral
      }

  override def getExternalDsName(modfqn: String): String =
    dsForName(modfqn) match {
      case None => notFound(modfqn, "cannot get external dataset name")
      case Some(ds: SmvExtDataSet) => ds.refname
      case _ => ""
    }

  override def getDqm(modfqn: String): SmvDQM =
    dsForName(modfqn) match {
      case None => notFound(modfqn, "cannot get dqm")
      case Some(ds) => ds.createDsDqm()
    }

  private def notFound(modfqn: String, msg: String) =
    throw new IllegalArgumentException(s"dataset [${modfqn}] is not found in ${getClass.getName}: ${msg}")

  override def dependencies(modfqn: String): String =
    if (isExternal(modfqn)) "" else dsForName(modfqn) match {
      case None => notFound(modfqn, "cannot get dependencies")
      case Some(ds) => ds.requiresDS.map(_.name).mkString(",")
    }

  override def getDataFrame(modfqn: String, validator: DQMValidator,
    known: java.util.Map[String, DataFrame]): DataFrame =
    dsForName(modfqn) match {
      case None => notFound(modfqn, "cannot get dataframe")
      case Some(ds) => ds.doRun(validator, known)
    }

  override def rerun(modfqn: String, validator: DQMValidator,
    known: java.util.Map[String, DataFrame]): DataFrame = {
    val parentClassLoader = getClass.getClassLoader
    val cl = class_loader.SmvClassLoader(SmvApp.app.smvConfig, parentClassLoader)
    val ds = new SmvReflection(cl).objectNameToInstance[SmvDataSet](modfqn)
    val message = shell.ShellCmd.hotdeployIfCapable(ds, parentClassLoader)
    println(message)
    scalaDataSets = scalaDataSets + (modfqn -> ds)
    ds.doRun(validator, known)
  }

  override def datasetHash(modfqn: String, includeSuperClass: Boolean = true): Long =
    dsForName(modfqn) match {
      case None => notFound(modfqn, "cannot calc dataset hash")
      case Some(ds) => ds.datasetCRC
    }
}
