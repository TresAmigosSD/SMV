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
    val helper = new SchemaDiscoveryHelper(sqlContext)
    val schema = helper.discoverSchema(strRDD,10, CsvAttributes.defaultCsvWithHeader)
    val entries = schema.entries

    assert(entries.length === 8)

    assert(entries(0).field.name === "id")
    assert(entries(0).typeFormat.typeName === "Integer")
    assert(entries(1).field.name === "name")
    assert(entries(1).typeFormat.typeName === "String")
    assert(entries(2).field.name === "age")
    assert(entries(2).typeFormat.typeName === "Integer")
    assert(entries(3).field.name === "weight")
    assert(entries(3).typeFormat.typeName === "Float")
    assert(entries(4).field.name === "active")
    assert(entries(4).typeFormat.typeName === "Boolean")
    assert(entries(5).field.name === "address")
    assert(entries(5).typeFormat.typeName === "String")
    assert(entries(6).field.name === "registration_date")
    assert(entries(6).typeFormat.typeName === "Timestamp")
    assert(entries(7).field.name === "last_active_date")
    assert(entries(7).typeFormat.typeName === "Timestamp")
  }

  test("Test schema discovery no header") {
    val strRDD = sqlContext.sparkContext.textFile(testDataDir +  "SchemaDiscoveryTest/test2.csv")
    val helper = new SchemaDiscoveryHelper(sqlContext)
    val schema = helper.discoverSchema(strRDD,10, CsvAttributes())
    val entries = schema.entries

    assert(entries.length === 5)

    assert(entries(0).field.name === "f1")
    assert(entries(0).typeFormat.typeName === "Integer")
    assert(entries(1).field.name === "f2")
    assert(entries(1).typeFormat.typeName === "String")
    assert(entries(2).field.name === "f3")
    assert(entries(2).typeFormat.typeName === "Integer")
    assert(entries(3).field.name === "f4")
    assert(entries(3).typeFormat.typeName === "Float")
    assert(entries(4).field.name === "f5")
    assert(entries(4).typeFormat.typeName === "Boolean")
  }

  test("Test schema discovery type promotion") {
    val strRDD = sqlContext.sparkContext.textFile(testDataDir +  "SchemaDiscoveryTest/test3.csv")
    val helper = new SchemaDiscoveryHelper(sqlContext)
    val schema = helper.discoverSchema(strRDD,10, CsvAttributes.defaultCsvWithHeader)
    val entries = schema.entries

    assert(entries.length === 6)

    assert(entries(0).field.name === "id")
    assert(entries(0).typeFormat.typeName === "Long")
    assert(entries(1).field.name === "name")
    assert(entries(1).typeFormat.typeName === "String")
    assert(entries(2).field.name === "age")
    assert(entries(2).typeFormat.typeName === "Integer")
    assert(entries(3).field.name === "salary")
    assert(entries(3).typeFormat.typeName === "Float")
    assert(entries(4).field.name === "active")
    assert(entries(4).typeFormat.typeName === "String")
    assert(entries(5).field.name === "last_active_date")
    assert(entries(5).typeFormat.typeName === "String")
  }

  test("Test schema discovery with parser errors") {
    val strRDD = sqlContext.sparkContext.textFile(testDataDir + "SchemaDiscoveryTest/test4.csv")
    val helper = new SchemaDiscoveryHelper(sqlContext)
    val schema = helper.discoverSchema(strRDD, 10, CsvAttributes.defaultCsvWithHeader)
    val entries = schema.entries

    assert(entries.length === 3)
    assert(entries(0).field.name === "id")
    assert(entries(0).typeFormat.typeName === "Integer")
    assert(entries(1).field.name === "name")
    assert(entries(1).typeFormat.typeName === "String")
    assert(entries(2).field.name === "age")
    assert(entries(2).typeFormat.typeName === "Integer")
  }

  test("Test schema discovery with parser errors on all data lines") {
    val strRDD = sqlContext.sparkContext.textFile(testDataDir + "SchemaDiscoveryTest/test5.csv")
    val helper = new SchemaDiscoveryHelper(sqlContext)
    intercept[IllegalStateException] {
      helper.discoverSchema(strRDD, 10, CsvAttributes.defaultCsvWithHeader)
    }
  }
}
