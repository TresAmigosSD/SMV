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

import org.apache.spark.sql.catalyst.expressions.Sum

class RollupCubeOpTest extends SparkTestUtil {

  test("Test cube bitmask creation") {
    assert(new RollupCubeOp(null, Seq('a,'b)).cubeBitmasks() === Seq(0,1,2))
    assert(new RollupCubeOp(null, Seq('a,'b,'c)).cubeBitmasks() === Seq(0,1,2,3,4,5,6))
  }

  sparkTest("Test getNonRollupCols") {
    val srdd = createSchemaRdd("a:String; b:String; c:String; d:String", "a,b,c,d")
    assert(new RollupCubeOp(srdd, Seq('b,'c)).getNonRollupCols() === Seq("a", "d"))
  }

  sparkTest("Test createSRDDWithSentinel") {
    val srdd = createSchemaRdd("a:String; b:String; c:String; d:Integer",
      "a,b,c,1;x,y,z,2")
    val op = new RollupCubeOp(srdd, Seq('b,'c))

    // test result with col c replaced with "*"
    val res_c = op.createSRDDWithSentinel(1)
    assertSrddDataEqual(res_c, "b,*,a,1;y,*,x,2")
    assertSrddSchemaEqual(res_c, "b:String; c:String; a:String; d:Integer")

    // test result with col b,c replaced with "*"
    val res_bc = op.createSRDDWithSentinel(3)
    assertSrddDataEqual(res_bc, "*,*,a,1;*,*,x,2")
    assertSrddSchemaEqual(res_bc, "b:String; c:String; a:String; d:Integer")
  }

  sparkTest("Test duplicateSRDDByBitmasks") {
    val srdd = createSchemaRdd("a:String; b:String; c:String; d:Integer", "a,b,c,1")
    val op = new RollupCubeOp(srdd, Seq('a,'b))

    val res = op.duplicateSRDDByBitmasks(Seq(0,1,2))
    assertSrddDataEqual(res, "a,b,c,1; *,b,c,1; a,*,c,1")
    assertSrddSchemaEqual(res, "a:String; b:String; c:String; d:Integer")
  }

  sparkTest("Test cube") {
    val srdd = createSchemaRdd("a:String; b:String; c:String; d:Integer",
      """a1,b1,cx,10;
         a1,b1,cx,20;
         a2,b2,cx,30;
         a1,b2,cx,40;
         a2,b2,cx,50""")
    import srdd.sqlContext._
    val res = new RollupCubeOp(srdd, Seq('a,'b), Seq(Sum('d) as 'sum_d)).cube

    assertSrddDataEqual(res,
      """a1,b1,30;
         a1,b2,40;
         a2,b2,80;
         *,b1,30;
         *,b2,120;
         a1,*,70;
         a2,*,80""")
    assertSrddSchemaEqual(res, "a:String; b:String; sum_d:Long")
  }
}
