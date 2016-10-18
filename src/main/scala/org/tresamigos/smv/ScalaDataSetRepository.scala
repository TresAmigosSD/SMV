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

  override def dependencies(modfqn: String): java.util.List[String] =
    dsForName(modfqn) match {
      case None => notFound(modfqn, "cannot get dependencies")
      case Some(ds) => ds.requiresDS.map(_.name)
    }

  override def getDataFrame(modfqn: String, modules: java.util.Map[String, DataFrame]): DataFrame =
    dsForName(modfqn) match {
      case None => notFound(modfqn, "cannot get dataframe")
      case Some(ds) => ??? // TODO may need to refactor so DQM and RunParams can have the same interface
    }
}
