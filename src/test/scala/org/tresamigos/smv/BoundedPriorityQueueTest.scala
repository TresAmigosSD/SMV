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

class BoundedPriorityQueueTest extends SmvTestUtil {
  test("Test bounded queue order") {
    val bpq = BoundedPriorityQueue[Int](3)
    List(5, 2, 4, 1, 3).foreach(i => bpq += i)
    assert(bpq.toList.sorted === List(3, 4, 5))
  }

  test("Test small bounded queue") {
    // create queue bigger than available num items.
    val bpq = BoundedPriorityQueue[Int](10)
    List(2, 5, 3, 1, 4).foreach(i => bpq += i)
    assert(bpq.toList.sorted === List(1, 2, 3, 4, 5))
  }

  test("Test duplicate values bounded queue") {
    val bpq = BoundedPriorityQueue[Int](4)
    List(3, 5, 2, 3, 4, 3, 1, 3).foreach(i => bpq += i)
    assert(bpq.toList.sorted === List(3, 3, 4, 5))
  }
}
