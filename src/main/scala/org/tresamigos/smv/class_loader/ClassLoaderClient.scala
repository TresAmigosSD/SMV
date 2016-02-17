package org.tresamigos.smv.class_loader

import org.eclipse.jetty.client.{Address, HttpExchange, ContentExchange, HttpClient}

/**
 * Base trait to be implemented by both local/remote class loader clients.
 */
trait ClassLoaderClientInterface {
  def getClassBytes(classFQN: String) : Array[Byte]
}

/**
 * Local class loader client that uses the ClassFinder directly to load class instead of going to
 * class loader server.
 */
class LocalClassLoaderClient(private val cmdLineArgs: Seq[String])
  extends ClassLoaderClientInterface {

  val config = new ClassLoaderConfig(cmdLineArgs)
  val classFinder = new ClassFinder(config.classDir)

  override def getClassBytes(classFQN: String) : Array[Byte] = {
    classFinder.getClassBytes(classFQN)
  }

}


/**
 * The real class loader client that connects to the remote class loader server to get the class bytes.
 */
class ClassLoaderClient(private val cmdLineArgs: Seq[String])
  extends ClassLoaderClientInterface {

  val config = new ClassLoaderConfig(cmdLineArgs)

  val httpClient = new HttpClient()
  httpClient.start()

  override def getClassBytes(classFQN: String) : Array[Byte] = {
    println("Starting client @" + config.port)

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

    if (exchangeState == HttpExchange.STATUS_COMPLETED) {
      println("Success:")
    } else if (exchangeState == HttpExchange.STATUS_EXCEPTED) {
      println("Excepted")
    } else if (exchangeState == HttpExchange.STATUS_EXPIRED) {
      println("Expired")
    }

    println("response status = " + exchange.getResponseStatus)
    val b = exchange.getResponseContentBytes
    b
  }

  def run() = {
    val b = getClassBytes("com.omnicis.lucentis.ui.CommonUI")
    b.slice(0,10).foreach { b => println(Integer.toHexString(b & 0xff))}
    val b2 = getClassBytes("com.omnicis.lucentis.ui.CommonUI$")
    b2.slice(0,10).foreach { b => println(Integer.toHexString(b & 0xff))}
  }
}

// For testing purposes only.  Remove eventually!!!
object ClassLoaderClient {
  def main(args: Array[String]): Unit = {
    val client = new ClassLoaderClient(args)
    client.run()
  }

}