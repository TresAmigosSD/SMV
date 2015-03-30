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

class ColumnHelperTest extends SparkTestUtil {
  sparkTest("test smvNullSub") {
    val ssc = sqlContext; import ssc.implicits._
    val srdd = sqlContext.createSchemaRdd("k:String; v:String;", "1,a;2,")
    val res = srdd.select($"v".smvNullSub("test"))
    assertSrddDataEqual(res, 
      "a;" +
      "test")
    val res2 = srdd.select($"v".smvNullSub($"k"))
    assertSrddDataEqual(res2, 
      "a;" +
      "2")
  }
  
  sparkTest("test smvLength"){
    val ssc = sqlContext; import ssc.implicits._
    val srdd = sqlContext.createSchemaRdd("k:String; v:String;", "1,a;2,")
    val res = srdd.select($"v".smvLength)
    assertSrddDataEqual(res, 
      "1;" +
      "null")
  }
  
  sparkTest("test smvStrToTimestamp"){
    val ssc = sqlContext; import ssc.implicits._
    val srdd = sqlContext.createSchemaRdd("k:String; v:String;", "20190101,a;,b")
    val res = srdd.select($"k".smvStrToTimestamp("yyyyMMdd"))
    assertSrddDataEqual(res, 
      "2019-01-01 00:00:00.0;" +
      "null")
  }
}
