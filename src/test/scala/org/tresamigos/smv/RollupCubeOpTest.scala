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
    assert(new RollupCubeOp(null, Nil, Seq("a","b")).cubeBitmasks() === Seq(0,1,2,3))
    assert(new RollupCubeOp(null, Nil, Seq("a","b","c")).cubeBitmasks() === Seq(0,1,2,3,4,5,6,7))
  }

  test("Test rollup bitmask creation") {
    assert(new RollupCubeOp(null, Nil, Seq("a","b")).rollupBitmasks() === Seq(0,1,3))
    assert(new RollupCubeOp(null, Nil, Seq("a","b","c")).rollupBitmasks() === Seq(0,1,3,7))
    assert(new RollupCubeOp(null, Nil, Seq("a","b","c", "d")).rollupBitmasks() === Seq(0,1,3,7,15))
  }

  test("Test getNonRollupCols") {
    val df = createSchemaRdd("a:String; b:String; c:String; d:String", "a,b,c,d")
    assert(new RollupCubeOp(df, Nil, Seq("b","c")).getNonRollupCols() === Seq("a", "d"))
  }

  test("Test createSRDDWithSentinel") {
    val df = createSchemaRdd("a:String; b:String; c:String; d:Integer",
      "a,b,c,1;x,y,z,2")
    val op = new RollupCubeOp(df, Nil, Seq("b","c"))

    // test result with col c replaced with "*"
    val res_c = op.createSRDDWithSentinel(1)
    assertSrddDataEqual(res_c, "b,null,a,1;y,null,x,2")

    // test result with col b,c replaced with "*"
    val res_bc = op.createSRDDWithSentinel(3)
    assertSrddDataEqual(res_bc, "null,null,a,1;null,null,x,2")
  }

  test("Test duplicateSRDDByBitmasks") {
    val df = createSchemaRdd("a:String; b:String; c:String; d:Integer", "a,b,c,1")
    val op = new RollupCubeOp(df, Nil, Seq("a","b"))

    val res = op.duplicateSRDDByBitmasks(Seq(0,1,2))
    assertSrddDataEqual(res, "a,b,c,1; null,b,c,1; a,null,c,1")
    assertSrddSchemaEqual(res, "a:String; b:String; c:String; d:Integer")
  }

  test("Test cube") {
    val df = createSchemaRdd("a:String; b:String; f:String; d:Integer",
      """a1,b1,F,10;
         a1,b1,F,20;
         a2,b2,G,30;
         a1,b2,F,40;
         a2,b2,G,50""")
    import df.sqlContext.implicits._

    val res = df.smvCube("f", "a", "b").agg(sum("d") as "sum_d").where($"f".isNotNull)
    assertSrddDataEqual(res,
      """F,a1,b2,40;
        G,a2,null,80;
        G,a2,b2,80;
        F,null,b1,30;
        F,null,null,70;
        F,null,b2,40;
        G,null,null,80;
        F,a1,b1,30;
        G,null,b2,80;
        F,a1,null,70""")
    assertSrddSchemaEqual(res, "f: String; a: String; b: String; sum_d: Long")

    val res2 = df.smvGroupBy("f").smvCube("a", "b").agg(sum("d") as "sum_d").select(
      "a", "b", "f", "sum_d"
    )

    assertSrddDataEqual(res2,
      """a1,b1,F,30;
         a1,b2,F,40;
         a1,null,F,70;
         null,b1,F,30;
         null,b2,F,40;
         a2,b2,G,80;
         a2,null,G,80;
         null,b2,G,80;
         null,null,F,70;
         null,null,G,80""")
    assertSrddSchemaEqual(res2, "a:String; b:String; f:String; sum_d:Long")
  }

  test("Test rollup") {
    val df = createSchemaRdd("a:String; b:String; c:String; d:Integer",
      """a1,b1,c1,10;
         a1,b1,c1,20;
         a1,b2,c2,30;
         a1,b2,c3,40;
         a2,b3,c4,50""")
    import df.sqlContext.implicits._

    val res = df.smvRollup("a", "b", "c").agg(sum("d") as "sum_d")
    assertSrddDataEqual(res,
    """a1,b1,c1,30;
       a1,b2,c2,30;
       a1,b2,c3,40;
       a2,b3,c4,50;
       a1,b1,null,30;
       a1,b2,null,70;
       a2,b3,null,50;
       a1,null,null,100;
       a2,null,null,50;
       null,null,null,150""")
    assertSrddSchemaEqual(res, "a:String; b:String; c:String; sum_d:Long")

    val res2 = df.smvGroupBy("a").smvRollup("b", "c").agg(sum("d") as "sum_d")
    assertSrddDataEqual(res2,
      """a1,b1,null,30;
         a1,b2,null,70;
         a2,b3,null,50;
         a1,b1,c1,30;
         a1,b2,c2,30;
         a1,b2,c3,40;
         a2,b3,c4,50;
         a1,null,null,100;
         a2,null,null,50""")
    assertSrddSchemaEqual(res2, "a:String; b:String; c:String; sum_d:Long")
  }

  test("test smvHierarchyAgg"){
    val ssc = sqlContext; import ssc.implicits._
    val df = createSchemaRdd("id:String;zip3:String;terr:String;div:String;v:Double",
    """a,100,001,01,10.3;
       a,102,001,01,1.0;
       a,102,001,01,2.0;
       a,102,001,01,3.0;
       b,102,001,01,4.0;
       b,103,001,01,10.1;
       b,103,001,01,10.2;
       a,103,001,01,10.3;
       a,201,002,01,11.0;
       b,301,003,02,15;
       b,401,004,02,15;
       b,405,004,02,20""")

    val res = df.smvGroupBy("id").smvHierarchyAgg("div", "terr", "zip3")(smvSum0($"v") as "v")
    assertSrddSchemaEqual(res, "id: String; hier_type: String; hier_value: String; v: Double")
    assertSrddDataEqual(res,
       """b,zip3,401,15.0;
          b,zip3,102,4.0;
          b,zip3,103,20.299999999999997;
          a,zip3,102,6.0;
          b,div,01,24.299999999999997;
          b,zip3,301,15.0;
          a,zip3,103,10.3;
          b,terr,004,35.0;
          a,div,01,37.6;
          b,div,02,50.0;
          b,terr,003,15.0;
          b,terr,001,24.299999999999997;
          a,zip3,201,11.0;
          a,terr,002,11.0;
          b,zip3,405,20.0;
          a,terr,001,26.6;
          a,zip3,100,10.3""")

  }
}
