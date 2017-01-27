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

class SmvPackageTest extends SmvUnitSpec {
  "urn2fqn" should "be an idempotent operation" in {
    Seq(("mod:a.b.c", "a.b.c"), ("link:x.y", "x.y"),
      ("bad:bad:bad:actor.in.ns", "actor.in.ns")) foreach {
      case (urn, fqn) =>
        urn2fqn(urn) shouldBe fqn
        urn2fqn(urn2fqn(urn)) shouldBe urn2fqn(urn)
    }
  }
}
