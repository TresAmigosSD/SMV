package org.tresamigos.smv.python

import org.apache.spark.sql._
import org.tresamigos.smv._

import scala.collection.JavaConversions._
import scala.util.{Try, Success, Failure}

/**
 * Provides RPC-style connector between Scala and Python modules.
 */
object SmvPythonRpc {
  object Commands {
    val RUN_MODULE = "run_module"
  }

  object Status {
    val Ok = "OK"
    val NotFound = "Not Found"
  }

  def ok(rest: (String, Any)*): Map[String, Any] = (rest :+ "status" -> Status.Ok).toMap

  def ok(df: DataFrame): Map[String, Any] = ok("dataframe" -> df)

  def notFound(msg: String) = Map("status" -> Status.NotFound, "message" -> msg)

  // WIP: compare this dispatch method with the specific methods below
  // which can also be called directly from Python
  def request(cmd: String, params: Map[String, Any]): java.util.Map[String, Any] = {

    // TODO: define comands
    cmd match {
      case Commands.RUN_MODULE =>
        val modname = params("modname").asInstanceOf[String]
        Try(SmvApp.app.dsForName(modname, getClass.getClassLoader)) match {
          case Success(ds) => ok(SmvApp.app.resolveRDD(ds))
          case Failure(_)  => notFound(modname)
        }
    }
  }

  // TODO: refactor dsforName to lookup in an app-level module registry first
  def resolve(modname: String): Try[SmvDataSet] =
     Try(SmvApp.app.dsForName(modname, getClass.getClassLoader))

  // This should add an entry in the app-level module registry for an external dataset
  def register(modname: String, language: String = "python"): SmvDataSet = ??? // ExtDataSet?
}
