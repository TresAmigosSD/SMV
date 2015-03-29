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
import org.apache.spark.sql.catalyst.types._

class SmvCDSTest extends SparkTestUtil {

  sparkTest("Test RunSum") {
    val ssc = sqlContext; import ssc._
    val srdd = createSchemaRdd("k:String; t:Integer; v:Double", "z,1,0.2;z,2,1.4;z,5,2.2;a,1,0.3;")

    val res = srdd.smvSingleCDSGroupBy('k)(TimeInLastN('t, 3))((Sum('v) as 'nv1), (Count('v) as 'nv2))
    assertSrddSchemaEqual(res, "k: String; t: Integer; nv1: Double; nv2: Long")
    assertUnorderedSeqEqual(res.collect.map(_.toString), Seq(
      "[a,1,0.3,1]",
      "[z,1,0.2,1]",
      "[z,2,1.5999999999999999,2]",
      "[z,5,2.2,1]"))
  }

  sparkTest("Test out of order CDS keys") {
    val ssc = sqlContext; import ssc._
    val srdd = createSchemaRdd("time_type:String;v:String;time_value:Integer",
      "k1,a,10;k1,b,100;k2,d,3;k2,c,12")

    val f = {in:Int => in - 3}
    val cds = SmvCDSRange(
      Seq('time_type, 'time_value),
      ('_time_value > ScalaUdf(f, IntegerType, Seq('time_value)) && ('_time_value <= 'time_value))
    )
    val s2 = srdd.selectMinus('time_type).selectPlus(Literal("MONTHR3") as 'time_type)
    val res = s2.smvSingleCDSGroupBy('v)(cds)(Count('v) as 'cv)
    assertSrddSchemaEqual(res, "v: String; time_type: String; time_value: Integer; cv: Long")
    assertUnorderedSeqEqual(res.collect.map(_.toString), Seq(
      "[d,MONTHR3,3,1]",
      "[c,MONTHR3,12,1]",
      "[a,MONTHR3,10,1]",
      "[b,MONTHR3,100,1]"))
  }

  sparkTest("Test SmvCDSRangeSelfJoin") {
    val ssc = sqlContext; import ssc._
    val srdd = createSchemaRdd("time_type:String;v:String;time_value:Integer",
      "k1,a,10;k1,b,100;k2,d,3;k2,c,12")

    val f = {in:Int => in - 3}
    val cds = SmvCDSRangeSelfJoin(
      Seq('time_type, 'time_value),
      ('_time_value > ScalaUdf(f, IntegerType, Seq('time_value)) && ('_time_value <= 'time_value))
    )
    val s2 = srdd.selectMinus('time_type).selectPlus(Literal("MONTHR3") as 'time_type)
    val res = s2.smvSingleCDSGroupBy('v)(cds)(Count('v) as 'cv)
    assertSrddSchemaEqual(res, "v: String; time_type: String; time_value: Integer; cv: Long")
    assertUnorderedSeqEqual(res.collect.map(_.toString), Seq(
      "[a,MONTHR3,12,1]",
      "[d,MONTHR3,3,1]",
      "[c,MONTHR3,12,1]",
      "[a,MONTHR3,10,1]",
      "[b,MONTHR3,100,1]"))
  }


  sparkTest("Test CDS chaining"){
    val ssc = sqlContext; import ssc._
    val srdd = sqlContext.createSchemaRdd("k:String; t:Integer; p: String; v:Double",
      """z,1,a,0.2;
         z,2,a,1.4;
         z,5,b,2.2;
         a,1,a,0.3""")
    val pCDS = PivotCDS(Seq(Seq('p)), Seq(('v, "v")), Seq("a", "b"))
    val res = srdd.smvApplyCDS('k)(TimeInLastN('t, 3)).
                   smvSingleCDSGroupBy('k, 't)(pCDS)(
                     Sum('v_a) as 'v_a,
                     Sum('v_b) as 'v_b
                   )
    assertSrddSchemaEqual(res, "k: String; t: Integer; v_a: Double; v_b: Double")
    assertUnorderedSeqEqual(res.collect.map(_.toString), Seq(
      "[a,1,0.3,0.0]",
      "[z,1,0.2,0.0]",
      "[z,2,1.5999999999999999,0.0]",
      "[z,5,0.0,2.2]"))
  }

  sparkTest("Test SmvCDSTopRec") {
    val ssc = sqlContext; import ssc._
    val srdd = sqlContext.createSchemaRdd("k:String; t:Integer; p: String; v:Double",
      """z,1,a,0.2;
         z,2,a,1.4;
         z,5,b,2.2;
         a,1,a,0.3""")
    val cds = SmvCDSTopRec('t.desc)
    val res=srdd.smvApplyCDS('k)(cds)

    assertSrddSchemaEqual(res, "k: String; t: Integer; p: String; v: Double")
    assertUnorderedSeqEqual(res.collect.map(_.toString), Seq(
      "[a,1,a,0.3]",
      "[z,5,b,2.2]"))
  }

  sparkTest("Test SmvCDSTopNRecs") {
    val ssc = sqlContext; import ssc._
    val srdd = sqlContext.createSchemaRdd("k:String; t:Integer; v:Double",
      """z,1,0.2;
         z,5,2.2;
         z,-5,0.8;
         z,3,1.1;
         z,2,1.4;
         a,1,0.3""")

    // test TopN (with descending ordering)
    val t_cds = SmvCDSTopNRecs(2, 't.desc)
    val t_res = srdd.smvApplyCDS('k)(t_cds)

    assertSrddSchemaEqual(t_res, "k: String; t: Integer; v: Double")
    assertUnorderedSeqEqual(t_res.collect.map(_.toString), Seq(
      "[a,1,0.3]",
      "[z,3,1.1]",
      "[z,5,2.2]"))

    // test BottomN (using ascending ordering)
    val b_cds = SmvCDSTopNRecs(2, 't.asc)
    val b_res = srdd.smvApplyCDS('k)(b_cds)

    assertSrddSchemaEqual(b_res, "k: String; t: Integer; v: Double")
    assertUnorderedSeqEqual(b_res.collect.map(_.toString), Seq(
      "[a,1,0.3]",
      "[z,1,0.2]",
      "[z,-5,0.8]"))


  }
}
