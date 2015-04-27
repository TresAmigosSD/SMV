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

import scala.util.Random
import org.apache.spark.sql.catalyst.expressions.Row


class SmvQuantileTest extends SparkTestUtil {
  sparkTest("Test smvDecile") {

    // Creates a test data string of the format "G1,0,k1,j1,1;G2,k2,j1,2;..." but that is randomly
    // shuffled to make sure the sort within quantile is working.
    val testData_1to20 = 1.to(20).map(n => s"G1,0,k${n},j${n},${n}")
    val testData_1to20_str = Random.shuffle(testData_1to20).mkString(";")

    // create the input srdd with 22 rows.
    val srdd = createSchemaRdd("g:String; g2:Integer; k:String; junk:String; v:Integer",
      testData_1to20_str + """;G2,0,x,J,10;G2,0,y,JJ,30""")

    val res = srdd.smvGroupBy('g, 'g2).smvMapGroup(new SmvQuantile("v", 10)).toDF
    //res.dumpSRDD

    /* expected output:
    [G1,0,k1,1,210.0,1.0,1]
    [G1,0,k2,2,210.0,3.0,1]
    [G1,0,k3,3,210.0,6.0,1]
    [G1,0,k4,4,210.0,10.0,1]
    [G1,0,k5,5,210.0,15.0,1]
    [G1,0,k6,6,210.0,21.0,2]
    [G1,0,k7,7,210.0,28.0,2]
    [G1,0,k8,8,210.0,36.0,2]
    [G1,0,k9,9,210.0,45.0,3]
    [G1,0,k10,10,210.0,55.0,3]
    [G1,0,k11,11,210.0,66.0,4]
    [G1,0,k12,12,210.0,78.0,4]
    [G1,0,k13,13,210.0,91.0,5]
    [G1,0,k14,14,210.0,105.0,6]
    [G1,0,k15,15,210.0,120.0,6]
    [G1,0,k16,16,210.0,136.0,7]
    [G1,0,k17,17,210.0,153.0,8]
    [G1,0,k18,18,210.0,171.0,9]
    [G1,0,k19,19,210.0,190.0,10]
    [G1,0,k20,20,210.0,210.0,10]
    [G2,0,x,10,40.0,10.0,3]
    [G2,0,y,30,40.0,40.0,10]
    */

    import res.sqlContext._
    val keyAndBin = res.select("k", "v_quantile").collect.map{ case Row(k:String, b:Int) => (k,b)}
    val expKeyAndBin = Seq[(String,Int)](
      ("k1",1),("k2",1),("k3",1),("k4",1),("k5",1),
      ("k6",2),("k7",2),("k8",2),("k9",3),("k10",3),
      ("k11",4),("k12",4),("k13",5),("k14",6),("k15",6),
      ("k16",7),("k17",8),("k18",9),("k19",10),("k20",10),
      ("x",3),("y",10)
    )

    assertUnorderedSeqEqual(keyAndBin, expKeyAndBin)
  }
}
