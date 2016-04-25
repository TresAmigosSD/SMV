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

package org.tresamigos.smv

import com.google.common.io.ByteStreams
import java.io._
import scala.tools.asm.ClassReader
import scala.tools.asm.util._

/**
 * Utility methods for analysis of class bytecode.
 */
object AsmUtil {
  /** Returns an asm-based text representation of the class bytecode */
  def asmTrace (reader: ClassReader): String = {
    val buf = new StringWriter
    val visitor = new TraceClassVisitor(null, new Textifier(), new PrintWriter(buf))

    /* SKIP_DEBUG: the visitLocalVariable and visitLineNumber methods will not be called. */
    reader.accept(visitor, ClassReader.SKIP_DEBUG)
    buf.toString
  }

  def asmTrace (fqn: String, cl: ClassLoader): String = {
    val in = cl.getResourceAsStream(fqn.replace('.', '/') + ".class")
    asmTrace(new ClassReader(getBytes(in)))
  }

  // a crude way to test if a class definition contains anonymous functions
  val AnonfunRegex = """\$anonfun\$""".r
  def hasAnonfun (fqn: String, cl: ClassLoader): Boolean = AnonfunRegex.findFirstIn(asmTrace(fqn, cl)).isDefined

  // TODO: this can be moved to a separate IO util module
  def getBytes (in: InputStream): Array[Byte] = try ByteStreams.toByteArray(in) finally in.close
}
