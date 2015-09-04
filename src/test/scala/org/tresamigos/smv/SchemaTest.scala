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

import scala.collection.SortedMap

// TODO: test writing of schema to file
// TODO: test reading/writing of data with different schema format (string quote, timestamp, etc).

class SmvSchemaTest extends SparkTestUtil {
  test("Test schema string parsing") {
    val s = SmvSchema.fromString("a:string; b:double")
    val entries = s.entries
    assert(entries.size === 2)
    assert(entries(0) === StringSchemaEntry("a"))
    assert(entries(1) === DoubleSchemaEntry("b"))
  }

  sparkTest("Test schema file parsing") {
    val s = SmvSchema.fromFile(sc, testDataDir +  "SchemaTest/test1.schema")
    val entries = s.entries
    assert(entries.size === 10)
    assert(entries(0) === StringSchemaEntry("id"))
    assert(entries(1) === DoubleSchemaEntry("val"))
    assert(entries(2) === TimestampSchemaEntry("val2"))
    assert(entries(3) === TimestampSchemaEntry("val3", "ddMMyyyy"))
    assert(entries(4) === LongSchemaEntry("val4"))
    assert(entries(5) === IntegerSchemaEntry("val5"))
    assert(entries(6) === BooleanSchemaEntry("val6"))
    assert(entries(7) === FloatSchemaEntry("val7"))
    assert(entries(8) === MapSchemaEntry("val8", StringSchemaEntry("keyType"), IntegerSchemaEntry("valType")))
    assert(entries(9) === ArraySchemaEntry("val9", IntegerSchemaEntry("valType")))
  }

  test("Schema entry equality") {
    val e1: SchemaEntry = StringSchemaEntry("a")
    val e2: SchemaEntry = StringSchemaEntry("a")
    val e3: SchemaEntry = StringSchemaEntry("b")
    val e4: SchemaEntry = DoubleSchemaEntry("a")

    assert(e1 == e2)
    assert(e1 != e3) // different name
    assert(e1 != e4) // different type
  }

  test("Test Timestamp Format") {
    val s = SmvSchema.fromString("a:timestamp[yyyy]; b:Timestamp[yyyyMMdd]; c:Timestamp[yyyyMMdd]")
    val a = s.entries(0)
    val b = s.entries(1)
    val c = s.entries(2)

    assert(a === TimestampSchemaEntry("a", "yyyy"))
    assert(c === TimestampSchemaEntry("c", "yyyyMMdd"))

    val date_a = a.valToStr(a.strToVal("2014"))
    val date_b = b.valToStr(b.strToVal("20140203"))
    assert(date_a === "2014-01-01 00:00:00.0") // 2014
    assert(date_b === "2014-02-03 00:00:00.0") // 20140203
  }

  test("Test Serialize Map Values") {
    val s = SmvSchema.fromString("a:map[integer, string]")
    val a = s.entries(0)

    assert(a === MapSchemaEntry("a", IntegerSchemaEntry("keyType"), StringSchemaEntry("valType")))

    val map_a = a.strToVal("1|2|3|4")
    assert(map_a === Map(1->"2", 3->"4"))

    // use a sorted map to ensure traversal order during serialization.
    val map_a_sorted = SortedMap(1->"2", 3->"4")
    val str_a = a.valToStr(map_a_sorted)
    assert(str_a === "1|2|3|4")
  }

  test("Test Serialize Array Values") {
    val s = SmvSchema.fromString("a:array[integer]")
    val a = s.entries(0)

    assert(a === ArraySchemaEntry("a", IntegerSchemaEntry("valType")))

    val array_a = a.strToVal("1|2|3|4")
    assert(array_a === Seq(1, 2, 3, 4))

    val array_a1 = Seq(4, 3, 2, 1)
    val str_a1 = a.valToStr(array_a1)
    assert(str_a1 === "4|3|2|1")

    val array_a2 = Seq(4, 3, 2, 1).toArray
    val str_a2 = a.valToStr(array_a2)
    assert(str_a2 === "4|3|2|1")
  }

  test("Test Serialize with null values") {
    val s = SmvSchema.fromString("a:integer; b:string")
    val a = s.entries(0)
    val b = s.entries(1)

    assert(a.valToStr(5) === "5")
    assert(a.valToStr(null) === "")
    assert(b.valToStr("x") === "x")
    assert(b.valToStr(null) === "")
  }

  sparkTest("Test Timestamp in file") {
    val df = open(testDataDir +  "SchemaTest/test2")
    assert(df.count === 3)
  }

  sparkTest("Test Timestamp default format") {
    val df = createSchemaRdd("a:Timestamp", "2011-09-03 10:13:58.0")
    assert(df.collect()(0)(0).toString === "2011-09-03 10:13:58.0")
    assert(SmvSchema.fromDataFrame(df).toString === "Schema: a: Timestamp[yyyy-MM-dd hh:mm:ss.S]")
  }

  sparkTest("Test Date default format") {
    val df = createSchemaRdd("a:Date", "2011-09-03")
    assertSrddSchemaEqual(df, "a: Date[yyyy-MM-dd]")
    assertSrddDataEqual(df, "2011-09-03")
  }

  test("Test schema name derivation from data file path") {
    assert(SmvSchema.dataPathToSchemaPath("/a/b/c.csv")    === "/a/b/c.schema")
    assert(SmvSchema.dataPathToSchemaPath("/a/b/c.tsv")    === "/a/b/c.schema")
    assert(SmvSchema.dataPathToSchemaPath("/a/b/c.csv.gz") === "/a/b/c.schema")
    assert(SmvSchema.dataPathToSchemaPath("/a/b/c")        === "/a/b/c.schema")

    // check that csv is only removed at end of string.
    assert(SmvSchema.dataPathToSchemaPath("/a/b/csv.foo") === "/a/b/csv.foo.schema")
  }

  test("Test mapping values to valid column names") {
    assert(SchemaEntry.valueToColumnName(" X Y Z ") === "X_Y_Z")
    assert(SchemaEntry.valueToColumnName("x_5/10/14 no! ") === "x_5_10_14_no")
    assert(SchemaEntry.valueToColumnName(55) === "55")
    assert(SchemaEntry.valueToColumnName(List(1.0, 2, 3).mkString(",")) === "1_0_2_0_3_0")
  }

  sparkTest("Test ArraySchema read and write") {
    val df = createSchemaRdd("a:Integer; b:Array[Double]",
      "1,0.3|0.11|0.1")

    assert(SmvSchema.fromDataFrame(df).toString === "Schema: a: Integer; b: Array[Double]")
    import df.sqlContext.implicits._
    val res = df.select($"b".getItem(0), $"b".getItem(1), $"b".getItem(2))

    assertDoubleSeqEqual(res.collect()(0).toSeq, Seq(0.3,0.11,0.1))
  }
}
