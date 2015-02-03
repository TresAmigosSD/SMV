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

import org.apache.spark.sql.catalyst.expressions._

class PivotTest extends SparkTestUtil {
  sparkTest("Test creation of unique column names") {
    val srdd = createSchemaRdd("k:String; p1:String; p2:String; p3:String; v:String; v2:Float",
      """x,p1_1,p2A,p3X,5,8;
         x,p1_2,p2A,p3X,6,9;
         x,p1_1,p2B,p3X,7,10""")

    val res = PivotOp.getBaseOutputColumnNames(srdd, Seq(Seq('p1, 'p2, 'p3)))
    assertUnorderedSeqEqual(res, Seq(
      "p1_1_p2A_p3X",
      "p1_1_p2B_p3X",
      "p1_2_p2A_p3X",
      "p1_2_p2B_p3X"))
  }

  sparkTest("Test creation of unique column names with missing/non-id data") {
    val srdd = createSchemaRdd("p1:String; p2:String; p3:String; v:String",
      """p1_1,p2/A,,5;
         p1_1,p2/A,p3X,6;
         ,p2/B,p3X,7""")

    val res = PivotOp.getBaseOutputColumnNames(srdd, Seq(Seq('p1, 'p2, 'p3)))
    assertUnorderedSeqEqual(res, Seq(
      "p1_1_p2_B_p3X",
      "p1_1_p2_B",
      "p1_1_p2_A_p3X",
      "p1_1_p2_A",
      "p2_B_p3X",
      "p2_B",
      "p2_A_p3X",
      "p2_A"))
  }

  sparkTest("Test creation of unique column names with 1 pivot column") {
    val srdd = createSchemaRdd("p1:String; v:String","p1_1,5; p1_2, 6")

    val res = PivotOp.getBaseOutputColumnNames(srdd, Seq(Seq('p1)))
    assertUnorderedSeqEqual(res, Seq(
      "p1_1",
      "p1_2"))
  }

  sparkTest("Test creation of smv pivot value column") {
    val srdd = createSchemaRdd("k:String; p1:String; p2:String; v:String; v2:String",
      "1,p1a,p2a,5,100; 1,p1b,p2b,6,200")

    val cds = PivotCDS(
      Seq(Seq('p1, 'p2)), 
      Seq(('v, "v"), ('v2, "v2")), 
      Seq("p1a_p2a", "p1a_p2b", "p1b_p2a", "p1b_p2b")
    )
    val res = srdd.smvApplyCDS('k)(cds)

    assertUnorderedSeqEqual(res.collect.map(_.toString), Seq(
      "[1,5,null,null,null,100,null,null,null]",
      "[1,null,null,null,6,null,null,null,200]"))

    val fieldNames = res.schema.fieldNames.toList
    assert(fieldNames === Seq("k", "v_p1a_p2a", "v_p1a_p2b", "v_p1b_p2a", 
      "v_p1b_p2b", "v2_p1a_p2a", "v2_p1a_p2b", "v2_p1b_p2a", "v2_p1b_p2b"))
  }

  sparkTest("Test pivot_sum function") {
    val srdd = createSchemaRdd("k:String; p1:String; p2:String; v:Integer",
      "1,p1/a,p2a,100;" +
      "1,p1!b,p2b,200;" +
      "1,p1/a,p2b,300;" +
      "1,p1!b,p2a,400;" +
      "1,p1/a,p2a,500;" + // same key as first row!
      "2,p1/a,p2a,600") // test with a single input row per key

    val res = srdd.pivot_sum('k, Seq('p1, 'p2), Seq('v))
    assertUnorderedSeqEqual(res.collect.map(_.toString), Seq(
      "[1,600,300,400,200]",
      "[2,600,0,0,0]"))

    val fieldNames = res.schema.fieldNames.toList
    assert(fieldNames === Seq("k", "v_p1_a_p2a", "v_p1_a_p2b", "v_p1_b_p2a", "v_p1_b_p2b"))
  }

  sparkTest("Test pivot_sum function with float value") {
    val srdd = createSchemaRdd("k:String; p:String; v1:Integer; v2:Float",
      "1,A,10,100.5;" +
      "1,A,20,200.5;" +
      "1,B,50,200.5;" +
      "2,A,60,500")

    val res = srdd.pivot_sum('k, Seq('p), Seq('v1, 'v2))
    assertUnorderedSeqEqual(res.collect.map(_.toString), Seq(
      "[1,30,50,301.0,200.5]",
      "[2,60,0,500.0,0.0]"))

    val fieldNames = res.schema.fieldNames.toList
    assert(fieldNames === Seq("k", "v1_A", "v1_B", "v2_A", "v2_B"))
  }

  sparkTest("Test pivot_sum function with multiple keys") {
    val srdd = createSchemaRdd("k1:String; k2:String; p:String; v1:Integer; v2:Float",
      "1,x,A,10,100.5;" +
      "1,y,A,10,100.5;" +
      "1,x,A,20,200.5;" +
      "1,x,B,50,200.5;" +
      "2,x,A,60,500")

    val res = srdd.pivot_sum('k1, 'k2)('p)('v1, 'v2)
    assertUnorderedSeqEqual(res.collect.map(_.toString), Seq(
      "[2,x,60,0,500.0,0.0]",
      "[1,x,30,50,301.0,200.5]",
      "[1,y,10,0,100.5,0.0]"))

    val fieldNames = res.schema.fieldNames.toList
    assert(fieldNames === Seq("k1", "k2", "v1_A", "v1_B", "v2_A", "v2_B"))
  }

  sparkTest("Test Pivot function with Sum and CountDistinct") {
    val ssc = sqlContext; import ssc._
    val srdd = createSchemaRdd("k1:String; k2:String; p:String; v1:Integer; v2:Float",
      "1,x,A,10,100.5;" +
      "1,y,A,10,100.5;" +
      "1,x,A,20,200.5;" +
      "1,x,A,10,200.5;" +
      "1,x,B,50,200.5;" +
      "2,x,A,60,500")

    val cds = PivotCDS(Seq(Seq('p)), Seq(('v1, "v1"), ('v2, "v2")), Seq("A", "B"))

    val res = srdd.smvSingleCDSGroupBy('k1, 'k2)(cds)(
      Sum('v1_A) as 'v1_A, 
      Sum('v1_B) as 'v1_B, 
      CountDistinct(Seq('v2_A)) as 'dist_cnt_v2_A, 
      CountDistinct(Seq('v2_B)) as 'dist_cnt_v2_B
    )

    assertUnorderedSeqEqual(res.collect.map(_.toString), Seq(
      "[2,x,60,0,1,0]",
      "[1,x,40,50,2,1]",
      "[1,y,10,0,1,0]"))

    val fieldNames = res.schema.fieldNames.toList
    assert(fieldNames === Seq("k1", "k2", "v1_A", "v1_B", "dist_cnt_v2_A", "dist_cnt_v2_B"))
  }

  sparkTest("Test smvPivot function with pivotColSets") {
    val ssc = sqlContext; import ssc._
    val srdd = createSchemaRdd("k1:String; k2:String; p:String; v1:Integer; v2:Float",
      "1,x,A,10,100.5;" +
      "1,y,A,10,100.5;" +
      "1,x,A,20,200.5;" +
      "1,x,A,10,200.5;" +
      "1,x,B,50,200.5;" +
      "2,x,A,60,500")

    val cds = PivotCDS(
      Seq(Seq('k2), Seq('k2, 'p)), 
      Seq(('v2, "v2")), 
      Seq("x", "x_A", "y_B")
    )

    val res = srdd.smvSingleCDSGroupBy('k1)(cds)(
      CountDistinct(Seq('v2_x)) as 'dist_cnt_v2_x, 
      CountDistinct(Seq('v2_x_A)) as 'dist_cnt_v2_x_A, 
      CountDistinct(Seq('v2_y_B)) as 'dist_cnt_v2_y_B 
    )

    assertUnorderedSeqEqual(res.collect.map(_.toString), Seq(
      "[1,2,2,0]",
      "[2,1,1,0]"))

    val fieldNames = res.schema.fieldNames.toList
    assert(fieldNames === Seq(
      "k1", 
      "dist_cnt_v2_x", 
      "dist_cnt_v2_x_A", 
      "dist_cnt_v2_y_B"))
  }

  sparkTest("Test smvPivotAddKnownOutput function with pivotColSets") {
    val ssc = sqlContext; import ssc._
    val srdd = createSchemaRdd("k1:String; k2:String; p:String; v1:Integer; v2:Float",
      "1,x,A,10,100.5;" +
      "1,y,A,10,100.5;" +
      "1,x,A,20,200.5;" +
      "1,x,A,10,200.5;" +
      "1,x,B,50,200.5;" +
      "2,x,A,60,500")

    val res = srdd.smvPivotAddKnownOutput(Seq('k2), Seq('k2, 'p))('v2)("x", "x_A", "y_B")
    assertSrddSchemaEqual(res, "k1: String; k2: String; p: String; v1: Integer; v2: Float; v2_x: Float; v2_x_A: Float; v2_y_B: Float")
    assertUnorderedSeqEqual(res.collect.map(_.toString), Seq(
      "[1,x,A,10,100.5,100.5,100.5,null]",
      "[1,y,A,10,100.5,null,null,null]",
      "[1,x,A,20,200.5,200.5,200.5,null]",
      "[1,x,A,10,200.5,200.5,200.5,null]",
      "[1,x,B,50,200.5,200.5,null,null]",
      "[2,x,A,60,500.0,500.0,500.0,null]"))
  }
}
