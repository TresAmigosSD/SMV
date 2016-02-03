package org.tresamigos.smv.class_loader

import org.eclipse.jetty.client.{Address, HttpExchange, ContentExchange, HttpClient}

class ClassLoaderClient(private val cmdLineArgs: Seq[String])
  extends ClassLoaderCommon(cmdLineArgs) {

  def run() = {
    val hostAndPort = host + ":" + port
    println("Starting client @" + hostAndPort)
    val httpClient = new HttpClient()
    httpClient.start()

    val exchange = new ContentExchange(true)
    exchange.setAddress(new Address(host, port))
    // TODO: convert class name string to URI encoding/quoting.
    exchange.setRequestURI("/class/com.omnicis.lucentis.ui.CommonUI")

    try {
      httpClient.send(exchange)
    } catch {
      case ce: Exception =>
        throw new IllegalStateException("Can not connect to Class Server @" + hostAndPort, ce)
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
    //    val s = exchange.getResponseContent
    //    println("response string = " + s)
    val b = exchange.getResponseContentBytes
    println("response string = ")
    b.slice(0,10).foreach { b => println(Integer.toHexString(b & 0xff))}
  }
}

object ClassLoaderClient {
  def main(args: Array[String]): Unit = {
    val client = new ClassLoaderClient(args)
    client.run()
  }

}