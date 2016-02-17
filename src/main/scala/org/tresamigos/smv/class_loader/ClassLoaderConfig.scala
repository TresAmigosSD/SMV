package org.tresamigos.smv.class_loader

import org.tresamigos.smv.SmvConfig

/**
 * Wrapper around class loader specific configuration.
 * Can be created from command line args or a previously created SmvConfig object.
 */
class ClassLoaderConfig(private val smvConfig: SmvConfig) {

  def this(cmdLineArgs: Seq[String]) = {
    this(new SmvConfig(cmdLineArgs))
  }

  val host = smvConfig.classServerHost
  val port = smvConfig.classServerPort
  val classDir = smvConfig.classServerClassDir
}

