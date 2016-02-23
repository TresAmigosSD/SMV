package org.tresamigos.smv.class_loader

import javax.servlet.http.{HttpServletResponse, HttpServletRequest}

import org.eclipse.jetty.server.{Request, Server}
import org.eclipse.jetty.server.handler._
import org.tresamigos.smv.SmvConfig

/**
 * The module/file class server.  This is the server end of the NetworkClassLoader and is used to serve class code / files.
 */
private[smv]
class ClassLoaderServer(private val smvConfig : SmvConfig) {

  val clConfig = new ClassLoaderConfig(smvConfig)

  // "singleton" used to find class byte code on the server.
  val classFinder = new ClassFinder(clConfig.classDir)

  def start() : Server = {
    println("Starting class server on port: " + clConfig.port)
    val server = new Server(clConfig.port)

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
    val smvConfig = new SmvConfig(args)
    val cs = new ClassLoaderServer(smvConfig)
    cs.start().join()
  }

}

/**
 * Handler for class code request.
 */
private[smv]
class ClassCodeRequestHandler(val classFinder: ClassFinder) extends AbstractHandler
{
  override def handle(target: String, baseRequest: Request, request: HttpServletRequest, httpResponse: HttpServletResponse) = {
//    println("baseRequest: " + baseRequest.toString)
//    println("  params = " + baseRequest.getParameterMap.toString)
//    println("  pathinfo = " + baseRequest.getPathInfo)
//    println("  param a = " + baseRequest.getParameter("a"))

    // get class name from request.
    val className = baseRequest.getPathInfo.stripPrefix("/")
//    println("Server load class:" + className)

    // load class bytes from disk.
    val classBytes = classFinder.getClassBytes(className)
    val resp = if (classBytes == null) {
      new ServerResponse(ServerResponse.STATUS_ERR_CLASS_NOT_FOUND)
    } else {
      // TODO: use file modification time as the file version
      new ServerResponse(10L, classBytes)
    }

    // send class bytes to client in response.
    httpResponse.setContentType("application/octet-stream")
    httpResponse.setStatus(HttpServletResponse.SC_OK)
    baseRequest.setHandled(true)
    resp.send(httpResponse.getOutputStream)
  }
}
