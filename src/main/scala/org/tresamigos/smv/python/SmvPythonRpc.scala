package org.tresamigos.smv.python

import scala.collection.JavaConversions._

/**
 * Provides RPC-style connector between Scala and Python modules.
 */
object SmvPythonRpc {
  def request(cmd: String, params: Map[String, Any]): java.util.Map[String, Any] = {

    // TODO: define comands
    cmd match {
      case "run_module" =>
    }

    params
  }
}
