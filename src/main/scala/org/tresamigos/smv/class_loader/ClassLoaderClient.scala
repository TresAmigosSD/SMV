package org.tresamigos.smv.class_loader

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
}


/**
 * The real class loader client that connects to the remote class loader server to get the class bytes.
 */
private[smv]
class ClassLoaderClient(private val config: ClassLoaderConfig)
  extends ClassLoaderClientInterface {

  val httpClient = new HttpClient()
  httpClient.start()

  override def getClassBytes(classFQN: String) : Array[Byte] = {
    val exchange = new ContentExchange(true)
    exchange.setAddress(new Address(config.host, config.port))
    exchange.setRequestURI("/class/" + classFQN)

    try {
      httpClient.send(exchange)
    } catch {
      case ce: Exception =>
        throw new IllegalStateException("Can not connect to Class Server @" + config.host + ":" + config.port, ce)
    }

    // Waits until the exchange is terminated
    val exchangeState = exchange.waitForDone()
    if (exchangeState != HttpExchange.STATUS_COMPLETED)
      throw new ClassNotFoundException(s"request to server returned ${exchangeState} for class ${classFQN}")
    if (exchange.getResponseStatus() != 200)
      throw new ClassNotFoundException(s"request to server completed with status of ${exchange.getResponseStatus()} for class ${classFQN}")

    val resp = ServerResponse(exchange.getResponseContentBytes)
    if (resp.status != ServerResponse.STATUS_OK)
      throw new ClassNotFoundException("class server did not find class: " + classFQN)
    resp.classBytes
  }
}
