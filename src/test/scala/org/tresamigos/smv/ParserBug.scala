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

class OpenCsvParserBug extends SparkTestUtil {

  test("excersize opencsv parser bug") {
    import scala.io.Source
    //import au.com.bytecode.opencsv.CSVParser
    val parser = new CSVParser(',')
    val testDataDir = "target/test-classes/data/"
    val res = for (line <- Source.fromFile(testDataDir + "ParserBug/test2.data").getLines())
      yield parser.parseLine(line).mkString("|")

    val expect = List("123|20140817|81.87|1234", "|20110203|91.20|", "123|20120301|901.22|")
    assert(res.toList === expect)
  }
}
