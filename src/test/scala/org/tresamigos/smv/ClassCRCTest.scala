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

import org.scalatest._

import scala.collection.mutable.ListBuffer
import scala.reflect.internal.util.BatchSourceFile
import scala.reflect.io.{AbstractFile, VirtualDirectory}
import scala.tools.nsc.reporters.StoreReporter
import scala.tools.nsc.{Settings, Global}

class ClassCRCTest extends FunSuite with Matchers {
  test("test two classes have different CRC") {
    val crc1 = ClassCRC("org.tresamigos.smv.ClassCRCTest")
    val crc2 = ClassCRC("org.tresamigos.smv.SmvModule")

    assert(crc1.crc != crc2.crc)
  }

  test("test same class has same CRC") {
    val crc1 = ClassCRC("org.tresamigos.smv.ClassCRCTest")
    val crc2 = ClassCRC("org.tresamigos.smv.ClassCRCTest")

    assert(crc1.crc == crc2.crc)
  }

  test("test for invalid class name CRC") {
    intercept[IllegalArgumentException] {
      val crc = ClassCRC("org.tresamigos.class_does_not_exist")
      crc.crc
    }
  }

  val compiler: Global = {
    val settings = new Settings()
    // settings.processArguments(List("-usejavacp"), processAll = true)
    settings.bootclasspath.append(sys.props("user.home") + "/.ivy2/cache/org.scala-lang/scala-library/jars/scala-library-2.10.4.jar")
    settings.bootclasspath.append(sys.props("user.home") + "/.ivy2/cache/org.scala-lang/scala-library/jars/scala-reflect-2.10.4.jar")
    settings.bootclasspath.append(sys.props("user.home") + "/.ivy2/cache/org.scala-lang/scala-library/jars/scala-compiler-2.10.4.jar")
    val compiler = new Global(settings, new StoreReporter)
    compiler.settings.outputDirs.setSingleOutput(new VirtualDirectory("(memory)", None))
    compiler
  }

  test("Adding comments to source code should not change bytecode checksum") {
    val crc1 = """object crc_1 {
    |  def run: Int = 1
    |}""".stripMargin

    val crc2 = crc1 + """
    |// adding comments should not change checksum""".stripMargin

    val res1 = singleClassBytecode(crc1)
    val res2 = singleClassBytecode(crc2)

    ClassCRC.checksum(res1).getValue shouldBe ClassCRC.checksum(res2).getValue
  }

  def singleClassBytecode(scalaCode: String): Array[Byte] = {
    new compiler.Run().compileSources(List(new BatchSourceFile("source.scala", scalaCode)))
    val res = getGeneratedClassfiles(compiler.settings.outputDirs.getSingleOutput.get)
    res(0)._2
  }

  def getGeneratedClassfiles(outDir: AbstractFile): List[(String, Array[Byte])] = {
    def files(dir: AbstractFile): List[(String, Array[Byte])] = {
      val res = ListBuffer.empty[(String, Array[Byte])]
      for (f <- dir.iterator) {
        if (!f.isDirectory) res += ((f.name, f.toByteArray))
        else if (f.name != "." && f.name != "..") res ++= files(f)
      }
      res.toList
    }
    files(outDir)
  }
}
