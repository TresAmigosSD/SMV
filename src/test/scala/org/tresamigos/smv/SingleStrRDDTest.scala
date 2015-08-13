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

class SingleStrRDDTest extends SparkTestUtil {

  sparkTest("Test hashSample, hashPartition and saveAsGZFile") {
    val f = sc.textFile(testDataDir + "SingleStrRDDTest/test1")
    val sampled=f.csvAddKey().hashSample(0.5) // index=0, delimiter=','
    assert(sampled.count === 2)
    // TODO: need to use the SmvHDFS helper to delete the generated file on cleanup.
    val random = scala.util.Random.nextInt(999999) // reduce chance of "file exist" issue of the second run
    val outf = testDataDir + s"/SingleStrRDDTest/out$random"
    sampled.csvAddKey(2).hashPartition(8).saveAsGZFile(outf)
    val newf = sc.textFile(outf + "/part-00002.gz")
    assert(newf.count === 1)
  }

}
