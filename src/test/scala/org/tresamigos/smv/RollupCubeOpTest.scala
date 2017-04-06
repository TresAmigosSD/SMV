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

import org.apache.spark.sql.functions._

class RollupCubeOpTest extends SmvTestUtil {

  test("Test cube bitmask creation") {
    assert(new RollupCubeOp(null, Nil, Seq("a", "b")).cubeBitmasks() === Seq(0, 1, 2, 3))
    assert(
      new RollupCubeOp(null, Nil, Seq("a", "b", "c")).cubeBitmasks() === Seq(0, 1, 2, 3, 4, 5, 6,
        7))
  }

  test("Test rollup bitmask creation") {
    assert(new RollupCubeOp(null, Nil, Seq("a", "b")).rollupBitmasks() === Seq(0, 1, 3))
    assert(new RollupCubeOp(null, Nil, Seq("a", "b", "c")).rollupBitmasks() === Seq(0, 1, 3, 7))
    assert(
      new RollupCubeOp(null, Nil, Seq("a", "b", "c", "d"))
        .rollupBitmasks() === Seq(0, 1, 3, 7, 15))
  }

  test("Test getNonRollupCols") {
    val df = dfFrom("a:String; b:String; c:String; d:String", "a,b,c,d")
    assert(new RollupCubeOp(df, Nil, Seq("b", "c")).getNonRollupCols() === Seq("a", "d"))
  }

  test("Test createSRDDWithSentinel") {
    val df = dfFrom("a:String; b:String; c:String; d:Integer", "a,b,c,1;x,y,z,2")
    val op = new RollupCubeOp(df, Nil, Seq("b", "c"))

    // test result with col c replaced with "*"
    val res_c = op.createSRDDWithSentinel(1)
    assertSrddDataEqual(res_c, "b,*,a,1;y,*,x,2")

    // test result with col b,c replaced with "*"
    val res_bc = op.createSRDDWithSentinel(3)
    assertSrddDataEqual(res_bc, "*,*,a,1;*,*,x,2")
  }

  test("Test duplicateSRDDByBitmasks") {
    val df = dfFrom("a:String; b:String; c:String; d:Integer", "a,b,c,1")
    val op = new RollupCubeOp(df, Nil, Seq("a", "b"), null)

    val res = op.duplicateSRDDByBitmasks(Seq(0, 1, 2))
    assertSrddDataEqual(res, "a,b,c,1; null,b,c,1; a,null,c,1")
    assertSrddSchemaEqual(res, "a:String; b:String; c:String; d:Integer")
  }

  test("Test cube") {
    val df = dfFrom("a:String; b:String; f:String; d:Integer", """a1,b1,F,10;
         a1,b1,F,20;
         a2,b2,G,30;
         a1,b2,F,40;
         a2,b2,G,50""")
    import df.sqlContext.implicits._

    val res = df.smvCube("f", "a", "b").agg(sum("d") as "sum_d").where($"f" =!= "*")

    assertSrddDataEqual(
      res,
      """F,a1,b2,40;
        G,a2,*,80;
        G,a2,b2,80;
        F,*,b1,30;
        F,*,*,70;
        F,*,b2,40;
        G,*,*,80;
        F,a1,b1,30;
        G,*,b2,80;
        F,a1,*,70"""
    )
    assertSrddSchemaEqual(res, "f: String; a: String; b: String; sum_d: Long")

    val res2 = df
      .smvGroupBy("f")
      .smvCube("a", "b")
      .agg(sum("d") as "sum_d")
      .select(
        "a",
        "b",
        "f",
        "sum_d"
      )

    assertSrddDataEqual(
      res2,
      """a1,b1,F,30;
         a1,b2,F,40;
         a1,*,F,70;
         *,b1,F,30;
         *,b2,F,40;
         a2,b2,G,80;
         a2,*,G,80;
         *,b2,G,80;
         *,*,F,70;
         *,*,G,80"""
    )
    assertSrddSchemaEqual(res2, "a:String; b:String; f:String; sum_d:Long")
  }

  test("Test rollup") {
    val df = dfFrom("a:String; b:String; c:String; d:Integer", """a1,b1,c1,10;
         a1,b1,c1,20;
         a1,b2,c2,30;
         a1,b2,c3,40;
         a2,b3,c4,50""")
    import df.sqlContext.implicits._

    val res = df.smvRollup("a", "b", "c").agg(sum("d") as "sum_d")
    assertSrddDataEqual(
      res,
      """a1,b1,c1,30;
       a1,b2,c2,30;
       a1,b2,c3,40;
       a2,b3,c4,50;
       a1,b1,*,30;
       a1,b2,*,70;
       a2,b3,*,50;
       a1,*,*,100;
       a2,*,*,50;
       *,*,*,150"""
    )
    assertSrddSchemaEqual(res, "a:String; b:String; c:String; sum_d:Long")

    val res2 = df.smvGroupBy("a").smvRollup("b", "c").agg(sum("d") as "sum_d")
    assertSrddDataEqual(
      res2,
      """a1,b1,*,30;
         a1,b2,*,70;
         a2,b3,*,50;
         a1,b1,c1,30;
         a1,b2,c2,30;
         a1,b2,c3,40;
         a2,b3,c4,50;
         a1,*,*,100;
         a2,*,*,50"""
    )
    assertSrddSchemaEqual(res2, "a:String; b:String; c:String; sum_d:Long")
  }
}
