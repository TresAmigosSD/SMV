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

class UnpivotTest extends SmvTestUtil {

  test("Test simple unpivot op") {
    val df = createSchemaRdd("id:String; X:String; Y:String; Z:String",
      """1,A,B,C; 2,D,E,F""")

    val res = df.smvUnpivot('X, 'Y, 'Z)
    assertSrddSchemaEqual(res, "id:String; column:String; value:String")
    assertSrddDataEqual(res,
    """1,X,A;1,Y,B;1,Z,C;
       2,X,D;2,Y,E;2,Z,F""")
  }
}