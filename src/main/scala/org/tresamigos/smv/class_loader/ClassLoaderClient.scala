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

import java.net.URLEncoder

import org.eclipse.jetty.client.{Address, HttpExchange, ContentExchange, HttpClient}

/**
 * Base trait to be implemented by both local/remote class loader clients.
 */
private[smv]
trait ClassLoaderClientInterface {
  /**
   * Get class bytes for the given class name.
   * Throws ClassNotFoundException of the class was not found.
   */
  def getClassBytes(classFQN: String) : Array[Byte]

  /**
   * Get the specified resource using the classpath on the server.
   */
  def getResourceBytes(resourcePath: String) : Array[Byte]
}


/**
 * Local class loader client that uses the ClassFinder directly to load class instead of going to
 * class loader server.
 */
private[smv]
class LocalClassLoaderClient(private val config: ClassLoaderConfig)
  extends ClassLoaderClientInterface {

  val classFinder = new ClassFinder(config.classDir)

  override def getClassBytes(classFQN: String) : Array[Byte] = {
    val b = classFinder.getClassBytes(classFQN)
    if (b == null)
      throw new ClassNotFoundException("LocalClassLoaderClient class not found: " + classFQN)
    b
  }

  override def getResourceBytes(resourcePath: String) : Array[Byte] = {
    val b = classFinder.getResourceBytes(resourcePath)
    if (b == null)
      throw new ClassNotFoundException("LocalClassLoaderClient resource not found: " + resourcePath)
    b
  }
}
