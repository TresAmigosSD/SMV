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
package matcher

import org.scalatest._

class StringMetricUdfTest extends SmvUnitSpec {
  "Levenshtein distance function" should "return normalized values" in {
    val (a, b) = ("abc", "abd")
    assume(com.rockymadden.stringmetric.similarity.LevenshteinMetric.compare(a, b) === Some(1))
    StringMetricUDFs.NormalizedLevenshteinFn("abc", "abd").get shouldEqual 2.0f/3 +- 1
  }

  it should "return None when either string is null or both are empty" in {
    StringMetricUDFs.NormalizedLevenshteinFn(null, "words") shouldBe None
    StringMetricUDFs.NormalizedLevenshteinFn(null, null) shouldBe None
    StringMetricUDFs.NormalizedLevenshteinFn("something", null) shouldBe None
    StringMetricUDFs.NormalizedLevenshteinFn("", "") shouldBe None
  }

  "Soundex function" should "return None when either string is null or both are empty" in {
    StringMetricUDFs.SoundexFn(null, "words") shouldBe None
    StringMetricUDFs.SoundexFn(null, null) shouldBe None
    StringMetricUDFs.SoundexFn("something", null) shouldBe None
    StringMetricUDFs.SoundexFn("", "") shouldBe None
  }

  "nGram2 function" should "return None when either string is null or both are empty" in {
    StringMetricUDFs.NGram2Fn(null, "words") shouldBe None
    StringMetricUDFs.NGram2Fn(null, null) shouldBe None
    StringMetricUDFs.NGram2Fn("something", null) shouldBe None
    StringMetricUDFs.NGram2Fn("", "") shouldBe None
  }

  "nGram3 function" should "return None when either string is null or both are empty" in {
    StringMetricUDFs.NGram3Fn(null, "words") shouldBe None
    StringMetricUDFs.NGram3Fn(null, null) shouldBe None
    StringMetricUDFs.NGram3Fn("something", null) shouldBe None
    StringMetricUDFs.NGram3Fn("", "") shouldBe None
  }

  "DiceSorensenFn function" should "return None when either string is null or both are empty" in {
    StringMetricUDFs.DiceSorensenFn(null, "words") shouldBe None
    StringMetricUDFs.DiceSorensenFn(null, null) shouldBe None
    StringMetricUDFs.DiceSorensenFn("something", null) shouldBe None
    StringMetricUDFs.DiceSorensenFn("", "") shouldBe None
  }

  "JaroWinklerFn function" should "return None when either string is null or both are empty" in {
    StringMetricUDFs.JaroWinklerFn(null, "words") shouldBe None
    StringMetricUDFs.JaroWinklerFn(null, null) shouldBe None
    StringMetricUDFs.JaroWinklerFn("something", null) shouldBe None
    StringMetricUDFs.JaroWinklerFn("", "") shouldBe None
  }
}
