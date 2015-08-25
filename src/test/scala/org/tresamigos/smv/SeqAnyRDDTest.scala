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


class SeqAnyRDDTest extends SparkTestUtil {
  sparkTest("test SeqAnyRDD to DataFrame") {
    val ssc = sqlContext; import ssc.implicits._
    val rdd = sc.parallelize(Seq(Seq(1, "a"), Seq(2, "b"), Seq(3, "c")))
    val schema = SmvSchema.fromString("id:Integer; val:String")
    val df = sqlContext.applySchemaToSeqAnyRDD(rdd, schema)
    val res = df.select('val).map(_(0)).collect
    val exp: Array[Any] = Array("a", "b", "c")

    assert(res === exp)
  }
}

