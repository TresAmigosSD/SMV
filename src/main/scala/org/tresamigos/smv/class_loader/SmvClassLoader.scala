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

import scala.util.{Try, Success}

/**
 * Custom "network" class loader that enable the loading of modified class files
 * without the need to rebuild the app.
 */
private[smv] case class SmvClassLoader(val classFinder: ClassFinder,
                                       val parentClassLoader: ClassLoader)
    extends ClassLoader(parentClassLoader) {

  /**
   * Override the default loadClass behaviour to check the local class directory
   * first. We can't check parent first because spark is usually run with the app
   * fat jar which will have all the modules defined in it.
   */
  override def loadClass(classFQN: String): Class[_] = {

    val clazz: Class[_] = getClassLoadingLock(classFQN).synchronized {
      // see if we have a cached copy in the JVM
      val clazz0 = findLoadedClass(classFQN)

      val clazz1: Option[Class[_]] = if (clazz0 == null) {
        Try(findClass(classFQN)) match {
          case Success(cl: Class[_]) => Some(cl)
          case _                     => None
        }
      } else Some(clazz0)

      clazz1.getOrElse(getParent.loadClass(classFQN))
    }

    clazz
  }

  /**
   * Override the default findClass in ClassLoader to load the class using the class loader client.
   */
  override def findClass(classFQN: String): Class[_] = {
    val klassBytes = getClassBytes(classFQN)
    val klass      = defineClass(classFQN, klassBytes, 0, klassBytes.length)
    klass
  }

  private def getClassBytes(classFQN: String): Array[Byte] = {
    val b = classFinder.getClassBytes(classFQN)
    if (b == null)
      throw new ClassNotFoundException(s"SmvClassLoader class not found: $classFQN")
    b
  }

  /**
   * Get resource as a byte input stream.
   */
  override def getResourceAsStream(name: String): InputStream = {
    val bytes = getResourceBytes(name)
    new ByteArrayInputStream(bytes)
  }

  private def getResourceBytes(resourcePath: String): Array[Byte] = {
    val b = classFinder.getResourceBytes(resourcePath)
    if (b == null)
      throw new ClassNotFoundException(s"SmvClassLoader resource not found: $resourcePath")
    b
  }
}

private[smv] object SmvClassLoader {
  def apply(smvConfig: SmvConfig,
            parentClassLoader: ClassLoader = getClass.getClassLoader): ClassLoader = {
    val classDir = smvConfig.classDir
    if (!classDir.isEmpty)
      // network class loader with local client connection
      new SmvClassLoader(new ClassFinder(classDir), parentClassLoader)
    else
      // default jar class loader
      parentClassLoader
  }
}
