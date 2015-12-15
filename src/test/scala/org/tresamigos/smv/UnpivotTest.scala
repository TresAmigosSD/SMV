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

    val res = df.smvUnpivot("X", "Y", "Z")
    assertSrddSchemaEqual(res, "id:String; column:String; value:String")
    assertSrddDataEqual(res,
    """1,X,A;1,Y,B;1,Z,C;
       2,X,D;2,Y,E;2,Z,F""")
  }

  test("multi-column unpivot should be the inverse of pivot") {
    val df = createSchemaRdd("id:String;A_1:String;A_2:String;B_1:String;B_2:String",
      "1,1a1,1a2,1b1,1b2;2,2a1,2a2,2b1,2b2")

    val res = df.smvUnpivot(
      Seq("A_1", "A_2", "B_1", "B_2"),
      n => { val seq = n.split("_"); (seq(0), seq(1))}
    )
    assertSrddSchemaEqual(res, "id:String;Index:String;A:String;B:String")
    assertSrddDataEqual(res,
      """1,1,1a1,1b1;
        |1,2,1a2,1b2;
        |2,1,2a1,2b1;
        |2,2,2a2,2b2""".stripMargin)
  }

  test("multi-column unpivot should preserve and padd with null") {
    val df = createSchemaRdd("id:String;A_1:String;A_2:String;B_1:String",
      "1,1a1,1a2,1b1;2,2a1,2a2,2b1")

    val res = df.smvUnpivot(
      Seq("A_1", "A_2", "B_1"),
      n => { val seq = n.split("_"); (seq(0), seq(1))}
    )
    assertSrddSchemaEqual(res, "id:String;Index:String;A:String;B:String")
    assertSrddDataEqual(res,
      """1,1,1a1,1b1;
        |1,2,1a2,null;
        |2,1,2a1,2b1;
        |2,2,2a2,null""".stripMargin)
  }

  test("unpivoted dataframe should not have an index column if user specifies to skip") {
    val df = createSchemaRdd("id:String;A_1:String;A_2:String;B_1:String;B_2:String",
      "1,1a1,1a2,1b1,1b2;2,2a1,2a2,2b1,2b2")

    val res = df.smvUnpivot(
      Seq("A_1", "A_2", "B_1", "B_2"),
      n => { val seq = n.split("_"); (seq(0), seq(1))},
      None
    )
    assertSrddSchemaEqual(res, "id:String;A:String;B:String")
    assertSrddDataEqual(res,
      """1,1a1,1b1;
        |1,1a2,1b2;
        |2,2a1,2b1;
        |2,2a2,2b2""".stripMargin)
  }
}
