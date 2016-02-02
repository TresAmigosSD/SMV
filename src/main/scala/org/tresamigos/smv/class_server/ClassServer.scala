package org.tresamigos.smv.class_server

import javax.servlet.http.{HttpServletResponse, HttpServletRequest}

import org.eclipse.jetty.server.{Request, Server}
import org.eclipse.jetty.server.handler._

/**
 * The module/file class server.  This is the server end of the NetworkClassLoader and is used to serve class code / files.
 */
class ClassServer {
  def run() = {

//    addFilters(handlers, conf)
//
//    val collection = new ContextHandlerCollection
//    val gzipHandlers = handlers.map { h =>
//      val gzipHandler = new GzipHandler
//      gzipHandler.setHandler(h)
//      gzipHandler
//    }
//    collection.setHandlers(gzipHandlers.toArray)


    // TODO: add error handling!
    // TODO: get port from config!
    val server = new Server(9999)

    val ctxt1 = new ContextHandler("/class")
    ctxt1.setHandler(new ClassCodeRequestHandler("CLASS"))

    val ctxt2 = new ContextHandler("/asset")
    ctxt2.setHandler(new ClassCodeRequestHandler("ASSET")) // just for testing for now!!!

    val contexts = new ContextHandlerCollection()
    contexts.setHandlers(Array(ctxt1, ctxt2))

    server.setHandler(contexts)
    server.start()
    server.join()
  }

}

object ClassServer {
  def main(args: Array[String]): Unit = {
    println("Starting server: 9999")
    val cs = new ClassServer()
    cs.run()
  }

}

/**
 * Handler for class code request.
 */
class ClassCodeRequestHandler(val msg: String) extends AbstractHandler
{
  override def handle(target: String, baseRequest: Request, request: HttpServletRequest, response: HttpServletResponse) = {
//    println("baseRequest: " + baseRequest.toString)
//    println("  params = " + baseRequest.getParameterMap.toString)
//    println("  pathinfo = " + baseRequest.getPathInfo)
//    println("  param a = " + baseRequest.getParameter("a"))

    val className = baseRequest.getPathInfo.stripPrefix("/")
    println("LOAD CLASS:" + className)
//    response.setContentType("text/html;charset=utf-8")
    response.setContentType("application/octet-stream")
    response.setStatus(HttpServletResponse.SC_OK)
    baseRequest.setHandled(true)
    val bytes = ("Hello World: " + msg).getBytes("UTF-8")
    response.getOutputStream.write(bytes)
  }
}
