package org.tresamigos.smv.class_loader

import javax.servlet.http.{HttpServletResponse, HttpServletRequest}

import org.eclipse.jetty.server.{Request, Server}
import org.eclipse.jetty.server.handler._

/**
 * The module/file class server.  This is the server end of the NetworkClassLoader and is used to serve class code / files.
 */
class ClassLoaderServer(private val cmdLineArgs: Seq[String]) {

  val config = new ClassLoaderConfig(cmdLineArgs) // TODO: this should be passed in for testing!!!!

  // "singleton" used to find class byte code on the server.
  val classFinder = new ClassFinder(config.classDir)

  def start() : Server = {

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
    println("Starting class server on port: " + config.port)
    println("  Using class dir: " + config.classDir)
    val server = new Server(config.port)

    val ctxt1 = new ContextHandler("/class")
    ctxt1.setHandler(new ClassCodeRequestHandler(classFinder))

    val ctxt2 = new ContextHandler("/asset")
    ctxt2.setHandler(new ClassCodeRequestHandler(classFinder)) // just for testing for now!!!

    val contexts = new ContextHandlerCollection()
    contexts.setHandlers(Array(ctxt1, ctxt2))

    server.setHandler(contexts)
    server.start()
    server
  }
}

object ClassLoaderServer {
  def main(args: Array[String]): Unit = {
    val cs = new ClassLoaderServer(args)
    cs.start().join()
  }

}

/**
 * Handler for class code request.
 */
class ClassCodeRequestHandler(val classFinder: ClassFinder) extends AbstractHandler
{
  override def handle(target: String, baseRequest: Request, request: HttpServletRequest, response: HttpServletResponse) = {
//    println("baseRequest: " + baseRequest.toString)
//    println("  params = " + baseRequest.getParameterMap.toString)
//    println("  pathinfo = " + baseRequest.getPathInfo)
//    println("  param a = " + baseRequest.getParameter("a"))

    // get class name from request.
    val className = baseRequest.getPathInfo.stripPrefix("/")
    println("LOAD CLASS:" + className)

    // load class bytes from disk.
    val classBytes = classFinder.getClassBytes(className)

    // send class bytes to client in response.
    response.setContentType("application/octet-stream")
    response.setStatus(HttpServletResponse.SC_OK)
    baseRequest.setHandled(true)
    response.getOutputStream.write(classBytes)
  }
}
