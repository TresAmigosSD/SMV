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
    val strRDD  = sqlContext.sparkContext.textFile(testDataDir + "SchemaDiscoveryTest/test1.csv")
    val helper  = new SchemaDiscoveryHelper(sqlContext)
    val schema  = helper.discoverSchema(strRDD, 10, CsvAttributes.defaultCsvWithHeader)
    val entries = schema.entries

    assert(entries.length === 9)

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
    assert(entries(6).typeFormat.typeName === "Date")
    assert(entries(7).field.name === "last_active_date")
    assert(entries(7).typeFormat.typeName === "Date")
    assert(entries(8).field.name === "last_active_time")
    assert(entries(8).typeFormat.typeName === "Timestamp")
  }

  test("Test schema discovery no header") {
    val strRDD  = sqlContext.sparkContext.textFile(testDataDir + "SchemaDiscoveryTest/test2.csv")
    val helper  = new SchemaDiscoveryHelper(sqlContext)
    val schema  = helper.discoverSchema(strRDD, 10, CsvAttributes())
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
    val strRDD  = sqlContext.sparkContext.textFile(testDataDir + "SchemaDiscoveryTest/test3.csv")
    val helper  = new SchemaDiscoveryHelper(sqlContext)
    val schema  = helper.discoverSchema(strRDD, 10, CsvAttributes.defaultCsvWithHeader)
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
    val strRDD  = sqlContext.sparkContext.textFile(testDataDir + "SchemaDiscoveryTest/test4.csv")
    val helper  = new SchemaDiscoveryHelper(sqlContext)
    val schema  = helper.discoverSchema(strRDD, 10, CsvAttributes.defaultCsvWithHeader)
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

  test("Test schema discovery with first record as desc") {
    val strRDD  = sqlContext.sparkContext.textFile(testDataDir + "SchemaDiscoveryTest/test6.csv")
    val helper  = new SchemaDiscoveryHelper(sqlContext)
    val schema  = helper.discoverSchema(strRDD, 10, CsvAttributes.defaultCsvWithHeader)
    val entries = schema.entries

    assert(SmvKeys.getMetaDesc(schema.toStructType.apply("name").metadata) === "bob")
    assert(SmvKeys.getMetaDesc(schema.toStructType.apply("age").metadata) === "66")
  }

  test("Test basic getTypeFormat Timestamp discovery") {
    val helper = new SchemaDiscoveryHelper(sqlContext)
    assert(helper.getTypeFormat(null, "12/06/2012 10:22:14.0") === TimestampTypeFormat("MM/dd/yyyy HH:mm:ss.S"))
    assert(helper.getTypeFormat(null, "12/06/2012 10:22:14") === TimestampTypeFormat("MM/dd/yyyy HH:mm:ss"))
    assert(helper.getTypeFormat(null, "12-06-2012 10:22:14.0") === TimestampTypeFormat("MM-dd-yyyy HH:mm:ss.S"))
    assert(helper.getTypeFormat(null, "12-06-2012 10:22:14") === TimestampTypeFormat("MM-dd-yyyy HH:mm:ss"))
    assert(helper.getTypeFormat(null, "May/01/1905 23:11:01.125") === TimestampTypeFormat("MMM/dd/yyyy HH:mm:ss.S"))
    assert(helper.getTypeFormat(null, "May/01/1905 23:11:01") === TimestampTypeFormat("MMM/dd/yyyy HH:mm:ss"))
    assert(helper.getTypeFormat(null, "May-01-1905 23:11:01.125") === TimestampTypeFormat("MMM-dd-yyyy HH:mm:ss.S"))
    assert(helper.getTypeFormat(null, "May-01-1905 23:11:01") === TimestampTypeFormat("MMM-dd-yyyy HH:mm:ss"))
    assert(helper.getTypeFormat(null, "01-May-1905 23:11:01.125") === TimestampTypeFormat("dd-MMM-yyyy HH:mm:ss.S"))
    assert(helper.getTypeFormat(null, "01-May-1905 23:11:01") === TimestampTypeFormat("dd-MMM-yyyy HH:mm:ss"))
    assert(helper.getTypeFormat(null, "01May1905 23:11:01.125") === TimestampTypeFormat("ddMMMyyyy HH:mm:ss.S"))
    assert(helper.getTypeFormat(null, "01May1905 23:11:01") === TimestampTypeFormat("ddMMMyyyy HH:mm:ss"))
    assert(helper.getTypeFormat(null, "2018-01-01 10:22:14.0") === TimestampTypeFormat("yyyy-MM-dd HH:mm:ss.S"))
    assert(helper.getTypeFormat(null, "2018-01-01 10:22:14") === TimestampTypeFormat("yyyy-MM-dd HH:mm:ss"))
  }

  test("Test basic getTypeFormat Date discovery") {
    val helper = new SchemaDiscoveryHelper(sqlContext)
    assert(helper.getTypeFormat(null, "12/06/2012") === DateTypeFormat("MM/dd/yyyy"))
    assert(helper.getTypeFormat(null, "12-06-2012") === DateTypeFormat("MM-dd-yyyy"))
    assert(helper.getTypeFormat(null, "May/01/1905") === DateTypeFormat("MMM/dd/yyyy"))
    assert(helper.getTypeFormat(null, "May-01-1905") === DateTypeFormat("MMM-dd-yyyy"))
    assert(helper.getTypeFormat(null, "01-May-1905") === DateTypeFormat("dd-MMM-yyyy"))
    assert(helper.getTypeFormat(null, "01May1905") === DateTypeFormat("ddMMMyyyy"))
    assert(helper.getTypeFormat(null, "2018-01-01") === DateTypeFormat("yyyy-MM-dd"))
  }

  test("Test ambiguous month-date ordering discovery") { 
    val helper = new SchemaDiscoveryHelper(sqlContext)
    //When it is ambiguous between month and date, we only support month in front
    assert(helper.getTypeFormat(null, "05/01/1905") === DateTypeFormat("MM/dd/yyyy"))
    assert(helper.getTypeFormat(null, "15/01/1905") === StringTypeFormat())
  }

  test("Test Date format change between records") {
    val helper = new SchemaDiscoveryHelper(sqlContext)
    //When one record in one format and the second in a different format should default to StringType
    assert(helper.getTypeFormat(DateTypeFormat("MM-dd-yyyy"), "Jan-01-2001") === StringTypeFormat())
  }
}
