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

// Note: it is very hard to test the validity of the CRC code as it would require
// two compilation steps and a code injection in the middle.
// These tests are really just a sanity test to make sure the plumbing is connected.
class ClassCRCTest extends SparkTestUtil {
  test("test two classes have different CRC") {
    val crc1 = ClassCRC("org.tresamigos.smv.ClassCRCTest")
    val crc2 = ClassCRC("java.lang.String")

    assert(crc1.crc != crc2.crc)
  }

  test("test same class has same CRC") {
    val crc1 = ClassCRC("org.tresamigos.smv.ClassCRCTest")
    val crc2 = ClassCRC("org.tresamigos.smv.ClassCRCTest")

    assert(crc1.crc == crc2.crc)
  }

  test("test for invalid class name CRC") {
    intercept[IllegalArgumentException] {
      val crc = ClassCRC("org.tresamigos.class_does_not_exist")
      crc.crc
    }
  }
}
