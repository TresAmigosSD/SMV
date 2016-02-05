package org.tresamigos.smv.class_loader

import java.net.URL
import sun.misc.URLClassPath

/**
 * Finds a class on a path and retrieve the byte code associated with it.
 * TODO: Caching of data could be done here (in companion object class)
 */
class ClassFinder (val classDir: String) {
  // TODO: use classDir parameter as root of class search path (watch out for trailing "/")
  val urlClassPath = new URLClassPath(Array(new URL("file:./target/classes/")))
  urlClassPath.getURLs.foreach(println)

  /**
   * Loads the class bytes for a given class name FQN.
   */
  def getClassBytes(className: String) : Array[Byte] = {
    val classFileName = className.replace('.', '/').concat(".class")
    val resource = urlClassPath.getResource(classFileName, false)
    if (resource == null) {
      // TODO: handle invalid class name here!
      println("Resource is null")
    }
    resource.getBytes
  }
}
