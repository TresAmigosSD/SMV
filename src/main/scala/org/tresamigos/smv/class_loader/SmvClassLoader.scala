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

import java.io.{ByteArrayInputStream, InputStream}

import org.tresamigos.smv.SmvConfig

import scala.util.{Try, Success, Failure}

/**
 * Custom "network" class loader that will load classes from remote class loader server (or local).
 * This will enable the loading of modified class files without the need to rebuild the app.
 */
private[smv]
case class SmvClassLoader(val classFinder: ClassFinder, val parentClassLoader: ClassLoader)
  extends ClassLoader(parentClassLoader) {
  /**
   * Override the default loadClass behaviour to check against server first rather than parent class loader.
   * We can't check parent first because spark is usually run with the app fat jar which will have all the
   * modules defined in it so we will never hit the server.
   * However, this may cause too many requests to server for non-app classes.
   * That is why the loadFromParentOnly method is used to white-list some common classes that
   * we know will not be on the server.
   */
  override def loadClass(classFQN: String) : Class[_] = {
    var c : Class[_] = null

    getClassLoadingLock(classFQN).synchronized {
      // see if we have a cached copy in the JVM
      c = findLoadedClass(classFQN)

      if (c == null) {
        c = Try( findClass(classFQN) ) match {
          case Success(cl: Class[_]) => cl
          case Failure(e: ClassNotFoundException) => null
        }
      }

      if (c == null) {
        c = getParent.loadClass(classFQN)
      }
    }

    c
  }

  /**
   * Override the default findClass in ClassLoader to load the class using the class loader client.
   * Depending on which client we have (remote/local), this may connect to server or just search local dir.
   */
  override def findClass(classFQN: String) : Class[_] = {
    val klassBytes = getClassBytes(classFQN)
    val klass = defineClass(classFQN, klassBytes, 0, klassBytes.length)
    klass
  }

  private def getClassBytes(classFQN: String) : Array[Byte] = {
    val b = classFinder.getClassBytes(classFQN)
    if (b == null)
      throw new ClassNotFoundException("LocalClassLoaderClient class not found: " + classFQN)
    b
  }

  /**
   * Get resource from server as a byte input stream.
   * This ignores the standard search recommendation by looking at the parent and just gets the resource from the server.
   * We also don't bother to go to parent AFTER the server as this is only called to load resources we know are on the server.
   */
  override def getResourceAsStream (name: String) : InputStream = {
    val bytes = getResourceBytes(name)
    new ByteArrayInputStream(bytes)
  }

  private def getResourceBytes(resourcePath: String) : Array[Byte] = {
    val b = classFinder.getResourceBytes(resourcePath)
    if (b == null)
      throw new ClassNotFoundException("LocalClassLoaderClient resource not found: " + resourcePath)
    b
  }
}

private[smv]
object SmvClassLoader {
  def apply(smvConfig: SmvConfig, parentClassLoader: ClassLoader = getClass.getClassLoader) : ClassLoader = {
    val classDir = smvConfig.classDir
    if (! classDir.isEmpty) {
      // network class loader with local client connection
      new SmvClassLoader(ClassFinder(classDir), parentClassLoader)
    } else {
      // default jar class loader
      parentClassLoader
    }
  }
}
