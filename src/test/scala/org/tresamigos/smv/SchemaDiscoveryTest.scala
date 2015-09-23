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

// TODO: Still need to add more test cases mainly type promotion.
class SchemaDiscoveryTest extends SmvTestUtil {
  test("Test schema discovery 1 line header") {
    val strRDD = sqlContext.sparkContext.textFile(testDataDir +  "SchemaDiscoveryTest/test1.csv")
    val schema = sqlContext.discoverSchema(strRDD,10, CsvAttributes.defaultCsvWithHeader)
    val entries = schema.entries

    assert(entries.length === 8)

    assert(entries(0).name === "id")
    assert(entries(0).typeName === "Integer")
    assert(entries(1).name === "name")
    assert(entries(1).typeName === "String")
    assert(entries(2).name === "age")
    assert(entries(2).typeName === "Integer")
    assert(entries(3).name === "weight")
    assert(entries(3).typeName === "Float")
    assert(entries(4).name === "active")
    assert(entries(4).typeName === "Boolean")
    assert(entries(5).name === "address")
    assert(entries(5).typeName === "String")
    assert(entries(6).name === "registration_date")
    assert(entries(6).typeName === "Timestamp")
    assert(entries(7).name === "last_active_date")
    assert(entries(7).typeName === "Timestamp")
  }

  test("Test schema discovery no header") {
    val strRDD = sqlContext.sparkContext.textFile(testDataDir +  "SchemaDiscoveryTest/test2.csv")
    val schema = sqlContext.discoverSchema(strRDD,10, CsvAttributes())
    val entries = schema.entries

    assert(entries.length === 5)

    assert(entries(0).name === "f1")
    assert(entries(0).typeName === "Integer")
    assert(entries(1).name === "f2")
    assert(entries(1).typeName === "String")
    assert(entries(2).name === "f3")
    assert(entries(2).typeName === "Integer")
    assert(entries(3).name === "f4")
    assert(entries(3).typeName === "Float")
    assert(entries(4).name === "f5")
    assert(entries(4).typeName === "Boolean")
  }

  test("Test schema discovery type promotion") {
    val strRDD = sqlContext.sparkContext.textFile(testDataDir +  "SchemaDiscoveryTest/test3.csv")
    val schema = sqlContext.discoverSchema(strRDD,10, CsvAttributes.defaultCsvWithHeader)
    val entries = schema.entries

    assert(entries.length === 6)

    assert(entries(0).name === "id")
    assert(entries(0).typeName === "Long")
    assert(entries(1).name === "name")
    assert(entries(1).typeName === "String")
    assert(entries(2).name === "age")
    assert(entries(2).typeName === "Integer")
    assert(entries(3).name === "salary")
    assert(entries(3).typeName === "Float")
    assert(entries(4).name === "active")
    assert(entries(4).typeName === "String")
    assert(entries(5).name === "last_active_date")
    assert(entries(5).typeName === "String")
  }
}
