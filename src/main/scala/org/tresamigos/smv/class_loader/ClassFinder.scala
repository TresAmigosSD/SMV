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

package org.tresamigos.smv.class_loader

import java.net.URL
import sun.misc.URLClassPath

/**
 * Finds a class/file on a path and retrieve the byte code associated with it.
 */
class ClassFinder (val classDir: String) {
  val urlClassPath = new URLClassPath(Array(new URL("file:" + classDir + "/")))

  /**
   * Loads the class bytes for a given class name FQN.
   * @return class bytes or null if class was not found.
   */
  def getClassBytes(className: String) : Array[Byte] = {
    val classFileName = className.replace('.', '/').concat(".class")
    getResourceBytes(classFileName)
  }

  /**
   * Loads the given resouce/file as an array of bytes.
   * @return file contents as bytes or null if file was not found.
   */
  def getResourceBytes(resourcePath: String) : Array[Byte] = {
    val resource = urlClassPath.getResource(resourcePath, false)
    return if (resource == null) null else resource.getBytes
  }
}
