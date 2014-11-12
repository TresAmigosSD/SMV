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

class PivotTest extends SparkTestUtil {

  sparkTest("Test creation of unique column names") {
    val srdd = createSchemaRdd("k:String; p1:String; p2:String; p3:String; v:String; v2:Float",
      """x,p1_1,p2A,p3X,5,8;
         x,p1_2,p2A,p3X,6,9;
         x,p1_1,p2B,p3X,7,10""")

    val res = new PivotOp(srdd, 'k, Seq('p1, 'p2, 'p3), Seq('v, 'v2)).baseOutputColumnNames
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

    val res = new PivotOp(srdd, 'k, Seq('p1, 'p2, 'p3), Seq('v)).baseOutputColumnNames
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

    val res = new PivotOp(srdd, 'k, Seq('p1), Seq('v)).baseOutputColumnNames
    assertUnorderedSeqEqual(res, Seq(
      "p1_1",
      "p1_2"))
  }

  sparkTest("Test creation of smv pivot value column") {
    val srdd = createSchemaRdd("k:String; p1:String; p2:String; v:String; v2:String",
      "1,p1a,p2a,5,100; 1,p1b,p2b,6,200")

    val res = new PivotOp(srdd, 'k, Seq('p1, 'p2), Seq('v, 'v2)).addSmvPivotValColumn.collect
    assertUnorderedSeqEqual(res.map(_.toString), Seq(
      "[1,p1a_p2a,5,100]",
      "[1,p1b_p2b,6,200]"))
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

}
