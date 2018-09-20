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
  val colNameFn: String => (String, String) = s => {
    val seq = s.split("_")
    (seq(0), seq(1))
  }

  test("Test simple unpivot op") {
    val df = dfFrom("id:String; X:String; Y:String; Z:String", """1,A,B,C; 2,D,E,F""")

    val res = df.smvUnpivot("X", "Y", "Z")
    assertSrddSchemaEqual(res, "id:String; column:String; value:String")
    assertSrddDataEqual(res, """1,X,A;1,Y,B;1,Z,C;
       2,X,D;2,Y,E;2,Z,F""")
  }

  test("multi-column unpivot should be the inverse of pivot") {
    val df = dfFrom("id:String;A_1:String;A_2:String;B_1:String;B_2:String",
                    "1,1a1,1a2,1b1,1b2;2,2a1,2a2,2b1,2b2")

    val res = df.smvUnpivot(Seq("A_1", "A_2", "B_1", "B_2"), colNameFn)
    assertSrddSchemaEqual(res, "id:String;Index:String;A:String;B:String")
    assertSrddDataEqual(
      res,
      """1,1,1a1,1b1;
        |1,2,1a2,1b2;
        |2,1,2a1,2b1;
        |2,2,2a2,2b2""".stripMargin
    )
  }

  test("multi-column unpivot should preserve and padd with null") {
    val df = dfFrom("id:String;A_1:String;A_2:String;B_1:String", "1,1a1,1a2,1b1;2,2a1,2a2,2b1")

    val res = df.smvUnpivot(Seq("A_1", "A_2", "B_1"), colNameFn)
    assertSrddSchemaEqual(res, "id:String;Index:String;A:String;B:String")
    assertSrddDataEqual(
      res,
      """1,1,1a1,1b1;
        |1,2,1a2,null;
        |2,1,2a1,2b1;
        |2,2,2a2,null""".stripMargin
    )
  }

  test("unpivoted dataframe should not have an index column if user specifies to skip") {
    val df = dfFrom("id:String;A_1:String;A_2:String;B_1:String;B_2:String",
                    "1,1a1,1a2,1b1,1b2;2,2a1,2a2,2b1,2b2")

    val res = df.smvUnpivot(Seq("A_1", "A_2", "B_1", "B_2"), colNameFn, None)
    assertSrddSchemaEqual(res, "id:String;A:String;B:String")
    assertSrddDataEqual(res,
                        """1,1a1,1b1;
                          |1,1a2,1b2;
                          |2,2a1,2b1;
                          |2,2a2,2b2""".stripMargin)
  }

  test("index ordering should put 2 before 10") {
    val columns = for (a <- 'A' to 'C'; n <- 1 to 20) yield a + "_" + n
    val schema  = ("Id:String;" +: columns.map(_ + ":String")) mkString ";"

    val data = Stream.from(1).take(61) mkString ","
    val df   = dfFrom(schema, data)

    val res = df.smvUnpivot(columns, colNameFn)
    assertSrddSchemaEqual(res, "Id:String;Index:String;A:String;B:String;C:String")

    def nextRow(n: Int): Seq[Int] = Seq(n, n + 1, 20 + n + 1, 40 + n + 1)
    val rows                      = for (n <- 1 to 20; r = 1 +: nextRow(n)) yield r.mkString(",")
    assertSrddDataEqual(res, rows mkString ";")
  }
}
