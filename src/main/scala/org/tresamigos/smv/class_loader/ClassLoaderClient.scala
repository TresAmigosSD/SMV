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


/**
 * The real class loader client that connects to the remote class loader server to get the class bytes.
 */
private[smv]
class ClassLoaderClient(private val config: ClassLoaderConfig)
  extends ClassLoaderClientInterface {

  val httpClient = new HttpClient()
  httpClient.start()

  /**
   * get the class/resource from the server as byte array.
   * @param resourceType Can be either "class" or "resource" to indicate if we should look for class file or direct file resource.
   * @param resourceName the name of the class or resrouce file.
   */
  private def getBytesFromServer(resourceType: String, resourceName: String) : Array[Byte] = {
    val exchange = new ContentExchange(true)
    exchange.setAddress(new Address(config.host, config.port))
    exchange.setRequestURI("/asset/?name=" + URLEncoder.encode(resourceName, "UTF-8") + "&type=" + resourceType)

    try {
      httpClient.send(exchange)
    } catch {
      case ce: Exception =>
        throw new IllegalStateException("Can not connect to Class Server @" + config.host + ":" + config.port, ce)
    }

    // Waits until the exchange is terminated
    val exchangeState = exchange.waitForDone()

    if (exchangeState != HttpExchange.STATUS_COMPLETED)
      throw new ClassNotFoundException(s"request to server returned ${exchangeState} for resource ${resourceName}")
    if (exchange.getResponseStatus() != 200)
      throw new ClassNotFoundException(s"request to server completed with status of ${exchange.getResponseStatus()} for resource ${resourceName}")

    val resp = ServerResponse(exchange.getResponseContentBytes)
    if (resp.status != ServerResponse.STATUS_OK)
      throw new ClassNotFoundException("class server did not find resource: " + resourceName)

    resp.bytes
  }

  override def getClassBytes(classFQN: String) : Array[Byte] = {
    getBytesFromServer("class", classFQN)
  }

  override def getResourceBytes(resourcePath: String) : Array[Byte] = {
    getBytesFromServer("resource", resourcePath)
  }
}
