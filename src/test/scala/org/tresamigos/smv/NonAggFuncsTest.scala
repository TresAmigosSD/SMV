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

class NonAggFuncsTest extends SparkTestUtil {
  sparkTest("test smvStrCat") {
    val ssc = sqlContext; import ssc.implicits._
    val srdd = sqlContext.createSchemaRdd("k:String; v:String;", "1,a;2,")
    val res = srdd.select(smvStrCat($"v".smvNullSub("test"), $"k"))
    assertSrddDataEqual(res, 
      "a1;" +
      "test2")
  }
  
  sparkTest("test smvAsArray") {
    val ssc = sqlContext; import ssc.implicits._
    val srdd = sqlContext.createSchemaRdd("k:String; v:String;", "1,a;2,")
    val res = srdd.select(smvAsArray($"v".smvNullSub("test"), $"k"))
    assertSrddDataEqual(res, 
      "List(a, 1);" +
      "List(test, 2)")
  }
}
