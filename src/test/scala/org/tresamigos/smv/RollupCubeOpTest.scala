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
  }
}
