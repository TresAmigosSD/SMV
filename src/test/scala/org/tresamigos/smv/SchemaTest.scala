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

import org.apache.spark.sql.types.Decimal

import scala.collection.SortedMap

// TODO: test writing of schema to file
// TODO: test reading/writing of data with different schema format (string quote, timestamp, etc).

class SmvSchemaTest extends SmvTestUtil {
  test("Test schema string parsing") {
    val s       = SmvSchema.fromString("a:string; b:double")
    val entries = s.entries
    assert(entries.size === 2)
    assert(entries(0) === SchemaEntry("a", StringTypeFormat()))
    assert(entries(1) === SchemaEntry("b", DoubleTypeFormat()))
  }

  test("Test schema file parsing") {
    val s       = SmvSchema.fromFile(sc, testDataDir + "SchemaTest/test1.schema")
    val entries = s.entries
    assert(entries.size === 10)
    assert(entries(0) === SchemaEntry("id", StringTypeFormat()))
    assert(entries(1) === SchemaEntry("val", DoubleTypeFormat()))
    assert(entries(2) === SchemaEntry("val2", TimestampTypeFormat()))
    assert(entries(3) === SchemaEntry("val3", TimestampTypeFormat("ddMMyyyy")))
    assert(entries(4) === SchemaEntry("val4", LongTypeFormat()))
    assert(entries(5) === SchemaEntry("val5", IntegerTypeFormat()))
    assert(entries(6) === SchemaEntry("val6", BooleanTypeFormat()))
    assert(entries(7) === SchemaEntry("val7", FloatTypeFormat()))
    assert(
      entries(8) === SchemaEntry("val8", MapTypeFormat(StringTypeFormat(), IntegerTypeFormat())))
    assert(entries(9) === SchemaEntry("val9", ArrayTypeFormat(IntegerTypeFormat())))

    val atts = s.attributes
    assert(atts === Map("key1" -> "val1", "key2" -> "val2b"))
  }

  test("Schema entry equality") {
    val e1: SchemaEntry = SchemaEntry("a", StringTypeFormat())
    val e2: SchemaEntry = SchemaEntry("a", StringTypeFormat())
    val e3: SchemaEntry = SchemaEntry("b", StringTypeFormat())
    val e4: SchemaEntry = SchemaEntry("a", DoubleTypeFormat())

    assert(e1 == e2)
    assert(e1 != e3) // different name
    assert(e1 != e4) // different type
  }

  test("Test Decimal formats") {
    val s = SmvSchema.fromString("a:Decimal; b:Decimal[ 5 ]; c:decimal[8, 3]")
    val a = s.entries(0)
    val b = s.entries(1)
    val c = s.entries(2)

    assert(a === SchemaEntry("a", DecimalTypeFormat(10, 0)))
    assert(b === SchemaEntry("b", DecimalTypeFormat(5, 0)))
    assert(c === SchemaEntry("c", DecimalTypeFormat(8, 3)))

    assert(a.typeFormat.toString() === "Decimal[10,0]")
    assert(b.typeFormat.toString() === "Decimal[5,0]")
    assert(c.typeFormat.toString() === "Decimal[8,3]")
  }

  test("Test Decimal value serialize") {
    val decFormat = DecimalTypeFormat(10, 0)

    assert(decFormat.strToVal("1234") === Decimal("1234"))
    assert(decFormat.valToStr(Decimal("1234")) === "1234")
  }

  test("Test Decimal default format") {
    val df = dfFrom("a:Decimal", "1234")
    assertSrddSchemaEqual(df, "a: Decimal[10,0]")
    assertSrddDataEqual(df, "1234")
  }

  test("Test Timestamp Format") {
    val s = SmvSchema.fromString("a:timestamp[yyyy]; b:Timestamp[yyyyMMdd]; c:Timestamp[yyyyMMdd]")
    val a = s.entries(0)
    val b = s.entries(1)
    val c = s.entries(2)

    assert(a === SchemaEntry("a", TimestampTypeFormat("yyyy")))
    assert(c === SchemaEntry("c", TimestampTypeFormat("yyyyMMdd")))

    val date_a = a.typeFormat.valToStr(a.typeFormat.strToVal("2014"))
    val date_b = b.typeFormat.valToStr(b.typeFormat.strToVal("20140203"))
    assert(date_a === "2014-01-01 00:00:00.0") // 2014
    assert(date_b === "2014-02-03 00:00:00.0") // 20140203
  }

  test("Test Date Format") {
    val s = SmvSchema.fromString("a:Date[yyyyMMdd]")
    val a = s.entries(0)

    val d = a.typeFormat.valToStr(a.typeFormat.strToVal("20161206"))
    assert(d === "20161206")
  }

  test("Test Serialize Map Values") {
    val s = SmvSchema.fromString("a:map[integer, string]")
    val a = s.entries(0)

    assert(a === SchemaEntry("a", MapTypeFormat(IntegerTypeFormat(), StringTypeFormat())))

    val map_a = a.typeFormat.strToVal("1|2|3|4")
    assert(map_a === Map(1 -> "2", 3 -> "4"))

    // use a sorted map to ensure traversal order during serialization.
    val map_a_sorted = SortedMap(1 -> "2", 3 -> "4")
    val str_a        = a.typeFormat.valToStr(map_a_sorted)
    assert(str_a === "1|2|3|4")
  }

  test("Test Serialize Array Values") {
    val s = SmvSchema.fromString("a:array[integer]")
    val a = s.entries(0)

    assert(a === SchemaEntry("a", ArrayTypeFormat(IntegerTypeFormat())))

    val array_a = a.typeFormat.strToVal("1|2|3|4")
    assert(array_a === Seq(1, 2, 3, 4))

    val array_a1 = Seq(4, 3, 2, 1)
    val str_a1   = a.typeFormat.valToStr(array_a1)
    assert(str_a1 === "4|3|2|1")

    val array_a2 = Seq(4, 3, 2, 1).toArray
    val str_a2   = a.typeFormat.valToStr(array_a2)
    assert(str_a2 === "4|3|2|1")
  }

  test("Test Serialize with null values") {
    val s = SmvSchema.fromString("a:integer; b:string")
    val a = s.entries(0)
    val b = s.entries(1)

    assert(a.typeFormat.valToStr(5) === "5")
    assert(a.typeFormat.valToStr(null) === "")
    assert(b.typeFormat.valToStr("x") === "x")
    assert(b.typeFormat.valToStr(null) === "")
  }

  test("Test Timestamp in file") {
    val df = open(testDataDir + "SchemaTest/test2")
    assert(df.count === 3)
  }

  test("Test Timestamp default format") {
    val df = dfFrom("a:Timestamp", "2011-09-03 00:13:58.0;2011-09-03 01:13:58.0;2011-09-03 11:13:58.0;" +
      "2011-09-03 12:13:58.0;2011-09-03 13:13:58.0;2011-09-03 23:13:58.0;2011-09-03 24:13:58.0")

    assert(df.collect()(0)(0).toString === "2011-09-03 00:13:58.0")
    assert(df.collect()(1)(0).toString === "2011-09-03 01:13:58.0")
    assert(df.collect()(2)(0).toString === "2011-09-03 11:13:58.0")
    assert(df.collect()(3)(0).toString === "2011-09-03 12:13:58.0")
    assert(df.collect()(4)(0).toString === "2011-09-03 13:13:58.0")
    assert(df.collect()(5)(0).toString === "2011-09-03 23:13:58.0")
    assert(df.collect()(6)(0).toString === "2011-09-04 00:13:58.0")
    assert(SmvSchema.fromDataFrame(df).toString === "Schema: a: Timestamp[yyyy-MM-dd HH:mm:ss.S]")
  }

  test("Test Date default format") {
    val df = dfFrom("a:Date", "2011-09-03")
    assertSrddSchemaEqual(df, "a: Date[yyyy-MM-dd]")
    assertSrddDataEqual(df, "2011-09-03")
  }

  test(f"Test serialize Byte") {
    val fmt = ByteTypeFormat()
    assert(fmt.valToStr(123.toByte) === "123")
    assert(fmt.valToStr(null) === "")
  }

  test(f"Test deserialize Byte") {
    val fmt = ByteTypeFormat()
    assert(fmt.strToVal("123") === 123.toByte)
    assert(fmt.strToVal("") === null)
  }

  test(f"Test serialize Short") {
    val fmt = ShortTypeFormat()
    assert(fmt.valToStr(12345.toShort) === "12345")
    assert(fmt.valToStr(null) === "")
  }

  test(f"Test deserialize Short") {
    val fmt = ShortTypeFormat()
    assert(fmt.strToVal("12345") === 12345.toShort)
    assert(fmt.strToVal("") === null)
  }

  test("Test schema name derivation from data file path") {
    assert(SmvSchema.dataPathToSchemaPath("/a/b/c.csv") === "/a/b/c.schema")
    assert(SmvSchema.dataPathToSchemaPath("/a/b/c.tsv") === "/a/b/c.schema")
    assert(SmvSchema.dataPathToSchemaPath("/a/b/c.csv.gz") === "/a/b/c.schema")
    assert(SmvSchema.dataPathToSchemaPath("/a/b/c") === "/a/b/c.schema")

    // check that csv is only removed at end of string.
    assert(SmvSchema.dataPathToSchemaPath("/a/b/csv.foo") === "/a/b/csv.foo.schema")
  }

  test("Test mapping values to valid column names") {
    assert(SchemaEntry.valueToColumnName(" X Y Z ") === "X_Y_Z")
    assert(SchemaEntry.valueToColumnName("x_5/10/14 no! ") === "x_5_10_14_no")
    assert(SchemaEntry.valueToColumnName(55) === "55")
    assert(SchemaEntry.valueToColumnName(null) === "null")
    assert(SchemaEntry.valueToColumnName(List(1.0, 2, 3).mkString(",")) === "1_0_2_0_3_0")
  }

  test("Test ArraySchema read and write") {
    val df = dfFrom("a:Integer; b:Array[Double]", "1,0.3|0.11|0.1")

    assert(SmvSchema.fromDataFrame(df).toString === "Schema: a: Integer; b: Array[Double]")
    import df.sqlContext.implicits._
    val res = df.select($"b".getItem(0), $"b".getItem(1), $"b".getItem(2))

    assertDoubleSeqEqual(res.collect()(0).toSeq, Seq(0.3, 0.11, 0.1))
  }

  test("Test schema extractCsvAttributes") {
    val s  = SmvSchema.fromString("""
          @has-header = false;
          @delimiter = \t;
          @quote-char = |;
          a:string""")
    val ca = s.extractCsvAttributes()
    assert(ca === CsvAttributes('\t', '|', false))
  }

  test("Test schema extractCsvAttributes when semicolon as delimiter ") {
    val s  = SmvSchema.fromString("""
          @has-header = false;
          @delimiter = semicolon;
          @quote-char = |;
          a:string""")
    val ca = s.extractCsvAttributes()
    assert(ca === CsvAttributes(';', '|', false))
  }

  // test default values of extracted csv attributes.
  test("Test schema extractCsvAttributes defaults") {
    val s  = SmvSchema.fromString("a:string; b:double")
    val ca = s.extractCsvAttributes()
    assert(ca === CsvAttributes(',', '\"', true))
  }

  test("Test schema addCsvAttributes") {
    val s1 = SmvSchema.fromString("@delimiter = +; @foo=bar; a:string")
    val s2 = s1.addCsvAttributes(CsvAttributes(';', '^', false))
    val exp_att =
      Map("foo" -> "bar", "delimiter" -> "semicolon", "has-header" -> "false", "quote-char" -> "^")
    assert(s2.attributes === exp_att)
  }

  test("test metadata read and write") {
    val df = dfFrom("""k:String; t:Integer @metadata={"smvDesc":"the time sequence"}; v:Double""",
                    "z,1,0.2;z,2,1.4;z,5,2.2;a,1,0.3;")
    val smvSchema = SmvSchema.fromDataFrame(df)
    assert(smvSchema.toString === """Schema: k: String; t: Integer @metadata={"smvDesc":"the time sequence"}; v: Double""")
    assertUnorderedSeqEqual(
      smvSchema.toStringsWithMeta,
      Seq("k: String", """t: Integer @metadata={"smvDesc":"the time sequence"}""", "v: Double"))
  }
}
