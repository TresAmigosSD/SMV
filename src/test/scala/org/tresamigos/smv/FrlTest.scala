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

class FrlTest extends SmvTestUtil {
  test("test frlFile loader with NoOp rejectlogger") {
    object file extends SmvFrlFile("./" + testDataDir + "FrlTest/test") {
      override val failAtParsingError = false
    }

    val res = file.rdd

    assertSrddSchemaEqual(res, "id: String; v: String")
    assertUnorderedSeqEqual(res.collect.map(_.toString), Seq(
      "[12,34]",
      "[23,45]",
      "[12,00]",
      "[qa,da]"))
  }
}
