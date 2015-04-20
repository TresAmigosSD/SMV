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
import org.apache.spark.sql.types._

class SmvCDSTest extends SparkTestUtil {

  sparkTest("Test SmvCDSTopNRecs") {
    val ssc = sqlContext; import ssc.implicits._
    val srdd = sqlContext.createSchemaRdd("k:String; t:Integer; v:Double",
      """z,1,0.2;
         z,5,2.2;
         z,-5,0.8;
         z,3,1.1;
         z,2,1.4;
         z,,3.0;
         a,1,0.3""")

    // test TopN (with descending ordering)
    val t_cds = SmvCDSTopNRecs(2, 't.desc)
    val t_res = srdd.smvGroupBy('k).smvApplyCDS(t_cds).toDF

    assertSrddSchemaEqual(t_res, "k: String; t: Integer; v: Double")
    assertUnorderedSeqEqual(t_res.collect.map(_.toString), Seq(
      "[a,1,0.3]",
      "[z,3,1.1]",
      "[z,5,2.2]"))

    // test BottomN (using ascending ordering)
    val b_cds = SmvCDSTopNRecs(2, 't.asc)
    val b_res = srdd.smvGroupBy('k).smvApplyCDS(b_cds).toDF

    assertSrddSchemaEqual(b_res, "k: String; t: Integer; v: Double")
    assertUnorderedSeqEqual(b_res.collect.map(_.toString), Seq(
      "[a,1,0.3]",
      "[z,1,0.2]",
      "[z,-5,0.8]"))


  }
}
