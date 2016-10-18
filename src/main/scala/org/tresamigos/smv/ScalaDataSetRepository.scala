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
  private var content: Map[String, SmvDataSet] = Map.empty

  def dsForName(modfqn: String): Option[SmvDataSet] =
    content.get(modfqn) orElse {
      SmvReflection.findObjectByName[SmvDataSet](modfqn) match {
        case Success(ds) =>
          content = content + (modfqn -> ds)
          Some(ds)
        case Failure(_) =>
          None
      }
    }

  override def hasDataSet(modfqn: String): Boolean =
    dsForName(modfqn).isDefined

  @inline private def notFound(modfqn: String, msg: String) =
    throw new IllegalArgumentException(s"dataset [${modfqn}] is not found in ${getClass.getName}: ${msg}")

  override def dependencies(modfqn: String): String =
    dsForName(modfqn) match {
      case None => notFound(modfqn, "cannot get dependencies")
      case Some(ds) => ds.requiresDS.map(_.name).mkString(",")
    }

  override def getDataFrame(modfqn: String, modules: java.util.Map[String, DataFrame]): DataFrame =
    dsForName(modfqn) match {
      case None => notFound(modfqn, "cannot get dataframe")
      case Some(ds) => ??? // TODO may need to refactor so DQM and RunParams can have the same interface
    }
}
