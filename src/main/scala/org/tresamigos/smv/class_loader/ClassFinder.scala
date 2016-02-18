package org.tresamigos.smv.class_loader

import java.net.URL
import sun.misc.URLClassPath

/**
 * Finds a class on a path and retrieve the byte code associated with it.
 */
class ClassFinder (val classDir: String) {
  val urlClassPath = new URLClassPath(Array(new URL("file:" + classDir + "/")))

  /**
   * Loads the class bytes for a given class name FQN.
   * @return class bytes or null if class was not found.
   */
  def getClassBytes(className: String) : Array[Byte] = {
    val classFileName = className.replace('.', '/').concat(".class")
    val resource = urlClassPath.getResource(classFileName, false)
    return if (resource == null) null else resource.getBytes
  }
}
