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

    val ctxt1 = new ContextHandler("/asset")
    ctxt1.setHandler(new ClassCodeRequestHandler(classFinder))

    val contexts = new ContextHandlerCollection()
    contexts.setHandlers(Array(ctxt1))

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
//    println("  params = " + baseRequest.getParameterMap.toString)
//    println("  pathinfo = " + baseRequest.getPathInfo)

    // get asset name/type from request.
    val resName = baseRequest.getParameter("name")
    val resType = baseRequest.getParameter("type")

    val bytes = if (resType == "class") {
      classFinder.getClassBytes(resName)
    } else {
      classFinder.getResourceBytes(resName)
    }

    val resp = if (bytes == null) {
      new ServerResponse(ServerResponse.STATUS_ERR_CLASS_NOT_FOUND)
    } else {
      // TODO: use file modification time as the file version
      new ServerResponse(10L, bytes)
    }

    // send class bytes to client in response.
    httpResponse.setContentType("application/octet-stream")
    httpResponse.setStatus(HttpServletResponse.SC_OK)
    baseRequest.setHandled(true)
    resp.send(httpResponse.getOutputStream)
  }
}
