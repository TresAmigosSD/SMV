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

package org.tresamigos.smv.class_loader

import org.tresamigos.smv.SparkTestUtil

class ClassFinderTest extends SparkTestUtil {
  val classLoaderTestDir = testDataDir + "ClassLoaderTest"
  val finder = new ClassFinder(classLoaderTestDir)

  test("find valid class object") {
    val b = finder.getClassBytes("com.smv.Foo")

    // first 4 bytes of class should be "ca fe ba be"
    assert(b(0) === 0xca.toByte)
    assert(b(1) === 0xfe.toByte)
    assert(b(2) === 0xba.toByte)
    assert(b(3) === 0xbe.toByte)
  }

  test("find invalid class object") {
    val b = finder.getClassBytes("invalid.Foo")
    assert(b === null)
  }

  test("find valid resource object") {
    val b = finder.getResourceBytes("files/abc.txt")
    assert(b === "xyz\n".getBytes("UTF8"))
  }

  test("find invalid resource object") {
    val b = finder.getResourceBytes("not_there.txt")
    assert(b === null)
  }

}
