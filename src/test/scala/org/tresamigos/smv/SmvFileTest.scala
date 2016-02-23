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

import java.io.File

class SmvFileTest extends SmvTestUtil {
  override   def appArgs: Seq[String] = Seq(
    "-m", "None",
    "--data-dir", testcaseTempDir,
    "--input-dir", s"${testcaseTempDir}/input"
  )

  test("test SmvFile full path") {
    resetTestcaseTempDir()

    new File(testcaseTempDir, "input").mkdir()

    createTempFile("input/a.csv", "f1\na\n")
    createTempFile("input/a.schema", "f1: String")

    object File1 extends SmvCsvFile("a.csv")
    val res1 = File1.rdd

    object File2 extends SmvCsvFile("input/a.csv")
    val res2 = File2.rdd

    assertSrddDataEqual(res1, "a")
    assertSrddDataEqual(res2, "a")
  }

  test("test SmvModuleLink can link to an SmvFile"){
    resetTestcaseTempDir()

    new File(testcaseTempDir, "input").mkdir()

    createTempFile("input/a.csv", "f1\na\n")
    createTempFile("input/a.schema", "f1: String")

    object File3 extends SmvCsvFile("a.csv") with SmvOutput
    object Link1 extends SmvModuleLink(File3)
  }

  test("read dir using SmvMultiCsvFiles"){
    resetTestcaseTempDir()

    new File(testcaseTempDir, "input").mkdir()
    new File(testcaseTempDir, "input/data1").mkdir()
    createTempFile("input/data1/f1", "col1\na\n")
    createTempFile("input/data1/f2", "col1\nb\n")

    createTempFile("input/data1.schema", "col1:String\n")

    object Data1 extends SmvMultiCsvFiles("data1")

    val res = Data1.rdd
    assertSrddDataEqual(res, "a;b")
  }
}
