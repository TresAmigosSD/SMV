package org.tresamigos.smv.class_loader

import java.io.{DataOutputStream, ByteArrayOutputStream}
import javax.servlet.http.{HttpServletResponse, HttpServletRequest}

import org.eclipse.jetty.server.{Request, Server}
import org.eclipse.jetty.server.handler._
import org.tresamigos.smv.SmvConfig

/**
 * The module/file class server.  This is the server end of the NetworkClassLoader and is used to serve class code / files.
 */
class ClassLoaderServer(private val smvConfig : SmvConfig) {

  val clConfig = new ClassLoaderConfig(smvConfig)

  // "singleton" used to find class byte code on the server.
  val classFinder = new ClassFinder(clConfig.classDir)

  def start() : Server = {

    // TODO: investigate how to gzip the files if needed (perhaps a config param?) to reduce network load.
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
class ClassCodeRequestHandler(val classFinder: ClassFinder) extends AbstractHandler
{
  override def handle(target: String, baseRequest: Request, request: HttpServletRequest, response: HttpServletResponse) = {
//    println("baseRequest: " + baseRequest.toString)
//    println("  params = " + baseRequest.getParameterMap.toString)
//    println("  pathinfo = " + baseRequest.getPathInfo)
//    println("  param a = " + baseRequest.getParameter("a"))

    // get class name from request.
    val className = baseRequest.getPathInfo.stripPrefix("/")
    println("Server load class:" + className)

    // load class bytes from disk.
    val classBytes = classFinder.getClassBytes(className)

    // TODO: extract this into a protocol class!!!
    val baos = new ByteArrayOutputStream(classBytes.length + 100)
    val dos = new DataOutputStream(baos)
    dos.writeInt(55)
    dos.writeInt(classBytes.length)
    dos.write(classBytes)
    dos.close()
//    val raw = baos.toByteArray
//    print("server data: "); raw.slice(0,10).foreach { b => print(Integer.toHexString(b & 0xff) + ",")}; println("")
//    println("class bytes size:" + classBytes.size)
//    println("raw size: " + raw.size)

    // TODO: add compression to the byte stream as well (using GZipOutputStream)

    // send class bytes to client in response.
    response.setContentType("application/octet-stream")
    response.setStatus(HttpServletResponse.SC_OK)
    baseRequest.setHandled(true)
    baos.writeTo(response.getOutputStream)
  }
}
