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
import scala.collection.JavaConversions._
import scala.util.{Success, Failure}

/**
 * Repository for data sets written in Scala.
 */
class ScalaDataSetRepository extends SmvDataSetRepository {
  private var scalaDataSets: Map[String, SmvDataSet] = Map.empty
  private var dataframes: Map[SmvDataSet, DataFrame] = Map.empty

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

  override def getExternalDsName(modfqn: String): String =
    dsForName(modfqn) match {
      case None => notFound(modfqn, "cannot get external dataset name")
      case Some(ds: SmvExtDataSet) => ds.refname
      case _ => ""
    }

  private def notFound(modfqn: String, msg: String) =
    throw new IllegalArgumentException(s"dataset [${modfqn}] is not found in ${getClass.getName}: ${msg}")

  override def dependencies(modfqn: String): String =
    if (isExternal(modfqn)) "" else dsForName(modfqn) match {
      case None => notFound(modfqn, "cannot get dependencies")
      case Some(ds) => ds.requiresDS.map(_.name).mkString(",")
    }

  override def getDataFrame(modfqn: String, modules: java.util.Map[String, DataFrame]): DataFrame =
    dsForName(modfqn) match {
      case None => notFound(modfqn, "cannot get dataframe")
      case Some(ds) =>
        dataframes.get(ds) match {
          case Some(df) => df
          case None =>
            ds.doRun(new dqm.DQMValidator(ds.createDsDqm()), modules)
        }
    }

  override def datasetHash(modfqn: String, includeSuperClass: Boolean = true): Long =
    dsForName(modfqn) match {
      case None => notFound(modfqn, "cannot calc dataset hash")
      case Some(ds) => ds.datasetCRC
    }
}
