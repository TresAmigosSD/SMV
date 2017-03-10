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

package org.tresamigos

/** Hash calculation skips package org.tresamigos.smv, so we create a separate package for test modules */
package fixture {
  import org.tresamigos.smv._
  // bring in testDataDir
  object TestConf extends SmvTestUtil

  object Module1 extends SmvModule("test Module1") {
    override def version() = 1
    override def requiresDS() = Nil
    override def run(i: runParams) = null
  }

  object file extends SmvCsvFile("./" + TestConf.testDataDir +  "CsvTest/test1", CsvAttributes.defaultCsvWithHeader)
}

package smv {

  class ModuleCrcConsistencyTest extends SmvTestUtil {
    // While working on unification of SmvDataSet loading schemes we will be changing
    // SmvDataSet implementation several times, causing CRCs to change. Ignore CRC
    // and datasetHash tests until this process is complete.

    ignore("test moduleCrc changed or not") {
      assert(fixture.Module1.datasetCRC === 476910372)
    }

    ignore("test moduleCrc on SmvFile"){
      assert(fixture.file.datasetCRC === 24672138)
    }
  }

}
