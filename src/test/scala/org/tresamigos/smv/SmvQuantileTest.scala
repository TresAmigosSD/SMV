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
import org.apache.spark.sql.Row


class SmvQuantileTest extends SmvTestUtil {
  test("Test smvDecile") {

    // Creates a test data string of the format "G1,0,k1,j1,1;G2,k2,j1,2;..." but that is randomly
    // shuffled to make sure the sort within quantile is working.
    val testData_1to20 = 1.to(20).map(n => s"G1,0,k${n},j${n},${n}")
    val testData_1to20_str = Random.shuffle(testData_1to20).mkString(";")

    // create the input df with 22 rows.
    val df = createSchemaRdd("g:String; g2:Integer; k:String; junk:String; v:Integer",
      testData_1to20_str + """;G2,0,x,J,10;G2,0,y,JJ,30""" + ";g3,0,w1,j,1;g3,0,w2,j,1;g3,0,w3,j,3")

    val res = df.smvGroupBy("g", "g2").smvDecile("v")

    val keyAndBin = res.select("k", "v_quantile").collect.map{ case Row(k:String, b:Int) => (k,b)}
    val expKeyAndBin = Seq[(String,Int)](
      ("k1",1),("k2",1),("k3",2),("k4",2),("k5",3),
      ("k6",3),("k7",4),("k8",4),("k9",5),("k10",5),
      ("k11",6),("k12",6),("k13",7),("k14",7),("k15",8),
      ("k16",8),("k17",9),("k18",9),("k19",10),("k20",10),
      ("x",1),("y",10), // quantile value should start at 1 and end at numBins
      ("w1",1),("w2",1),("w3",10) // same column value should receive the same quantile
    )

    assertUnorderedSeqEqual(keyAndBin, expKeyAndBin)
  }
}
