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

import org.json4s.jackson.JsonMethods._

class ValidationTest extends SparkTestUtil {

  test("Test ValidationResult") {
    val v = ValidationResult(
      false,
      Seq(("p1", "many issues:\n Issue1: ...\n Issue2: ..."), ("p2", "Simpy issue")),
      Seq("log1", "log2")
    )
    val res = pretty(parse(v.prettyJSON))
    assert(res ===
"""{
  "passed" : false,
  "errorMessages" : [ {
    "p1" : "many issues:\n Issue1: ...\n Issue2: ..."
  }, {
    "p2" : "Simpy issue"
  } ],
  "checkLog" : [ "log1", "log2" ]
}""")
  }
}
