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

package org.tresamigos.smv.matcher

import org.scalatest._

class StringMetricUdfTest extends FlatSpec with Matchers {
  "Levenshtein distance function" should "return normalized values" in {
    val (a, b) = ("abc", "abd")
    assume(com.rockymadden.stringmetric.similarity.LevenshteinMetric.compare(a, b) === Some(1))
    StringMetricUDFs.NormalizedLevenshteinFn("abc", "abd").get shouldEqual 2.0/3 +- 1e-6
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
}
