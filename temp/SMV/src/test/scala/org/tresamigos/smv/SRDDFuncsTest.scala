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

import org.apache.spark.sql._, types._, functions._

class SelectWithReplaceTest extends SmvTestUtil {
  val fields = Seq("name:String", "friends:Integer")
  val schema = fields.mkString(";")
  val data   = Seq("Adam,1", "Beth,2", "Caleb,3", "David,4")

  def testDf(sqlContext: SQLContext): DataFrame =
    dfFrom(schema, data.mkString(";"))

  test("should add new columns without modification") {
    val input = testDf(sqlContext)
    val res   = input.selectWithReplace(input("friends") + 1 as "newfriends")
    assertSrddSchemaEqual(res, schema + ";newfriends:Integer")
    assertSrddDataEqual(res, "Adam,1,2;Beth,2,3;Caleb,3,4;David,4,5")
  }

  test("should overwrite existing column with the same name") {
    val input = testDf(sqlContext)
    val res   = input.selectWithReplace(input("friends") + 1 as "friends")
    assertSrddSchemaEqual(res, "name:String;friends:Integer")
    assertSrddDataEqual(res, "Adam,2;Beth,3;Caleb,4;David,5")
  }

  test("should accept a column aliased multiple times") {
    val input = testDf(sqlContext)
    val res   = input.selectWithReplace(input("friends") as "friends" as "friends")
    assertSrddSchemaEqual(res, schema)
    assertSrddDataEqual(res, data.mkString(";"))
  }
}

class SelectPlusMinusTest extends SmvTestUtil {
  test("test SelectPlus") {
    val ssc = sqlContext; import ssc.implicits._
    val df  = dfFrom("a:Double;b:Double", "1.0,10.0;2.0,20.0;3.0,30.0")
    val res = df.smvSelectPlus('b + 2.0 as 'bplus2)
    assertSrddDataEqual(res,
                        "1.0,10.0,12.0;" +
                          "2.0,20.0,22.0;" +
                          "3.0,30.0,32.0")
  }

  test("test SelectPlusPrefix") {
    val ssc = sqlContext; import ssc.implicits._
    val df  = dfFrom("a:Double;b:Double", "1.0,10.0;2.0,20.0;3.0,30.0")
    val res = df.selectPlusPrefix('b + 2.0 as 'bplus2)
    assertSrddDataEqual(res,
                        "12.0,1.0,10.0;" +
                          "22.0,2.0,20.0;" +
                          "32.0,3.0,30.0")
  }

  test("test SelectMinus") {
    val ssc = sqlContext; import ssc.implicits._
    val df  = dfFrom("a:Double;b:Double", "1.0,10.0;2.0,20.0;3.0,30.0")
    val res = df.smvSelectMinus("b")
    assertSrddDataEqual(res,
                        "1.0;" +
                          "2.0;" +
                          "3.0")
  }

  test("test smvExpandStruct") {
    val ssc      = sqlContext; import ssc.implicits._
    val df       = dfFrom("id:String;a:Double;b:Double", "a,1.0,10.0;a,2.0,20.0;b,3.0,30.0")
    val dfStruct = df.select($"id", struct("a", "b") as "c")
    val res      = dfStruct.smvExpandStruct("c")
    assertSrddDataEqual(res, "a,1.0,10.0;a,2.0,20.0;b,3.0,30.0")
  }
}

class smvRenameFieldTest extends SmvTestUtil {
  test("test rename fields") {
    val df = dfFrom("a:Integer; b:Double; c:String", "1,2.0,hello")

    val result = df.smvRenameField("a" -> "aa", "c" -> "cc")

    val fieldNames = result.schema.fieldNames
    assert(fieldNames === Seq("aa", "b", "cc"))
    assert(result.collect.map(_.toString) === Seq("[1,2.0,hello]"))
  }

  test("test rename to existing field") {
    val df = dfFrom("a:Integer; b:Double; c:String", "1,2.0,hello")

    val e = intercept[SmvRuntimeException] {
      val result = df.smvRenameField("a" -> "c")
    }
    assert(e.getMessage === "Rename to existing fields: c")
  }

  test("test prefixing field names") {
    val df = dfFrom("a:Integer; b:Double; c:String", "1,2.0,hello")

    val result = df.smvPrefixFieldNames("xx_")

    val fieldNames = result.schema.fieldNames
    assert(fieldNames === Seq("xx_a", "xx_b", "xx_c"))
    assert(result.collect.map(_.toString) === Seq("[1,2.0,hello]"))
  }

  test("test postfixing field names") {
    val df = dfFrom("a:Integer; b:Double; c:String", "1,2.0,hello")

    val result = df.postfixFieldNames("_xx")

    val fieldNames = result.schema.fieldNames
    assert(fieldNames === Seq("a_xx", "b_xx", "c_xx"))
    assert(result.collect.map(_.toString) === Seq("[1,2.0,hello]"))
  }
}

class JoinHelperTest extends SmvTestUtil {
  test("test joinUniqFieldNames") {
    val ssc   = sqlContext; import ssc.implicits._
    val srdd1 = dfFrom("a:Integer; b:Double; c:String", """1,2.0,hello;
         1,3.0,hello;
         2,10.0,hello2;
         2,11.0,hello3""")

    val srdd2 = dfFrom("a2:Integer; c:String", """1,asdf;
         2,asdfg""")

    val result     = srdd1.joinUniqFieldNames(srdd2, $"a" === $"a2", "inner")
    val fieldNames = result.columns
    assert(fieldNames === Seq("a", "b", "c", "a2", "_c"))
    assertUnorderedSeqEqual(result.collect.map(_.toString),
                            Seq("[1,2.0,hello,1,asdf]",
                                "[1,3.0,hello,1,asdf]",
                                "[2,10.0,hello2,2,asdfg]",
                                "[2,11.0,hello3,2,asdfg]"))
  }

  test("test joinUniqFieldNames with ignore case") {
    val ssc   = sqlContext; import ssc.implicits._
    val srdd1 = dfFrom("a:Integer; b:Double; C:String", """1,2.0,hello;
      2,11.0,hello3""")

    val srdd2 = dfFrom("a2:Integer; c:String", """1,asdf;
      2,asdfg""")

    val result = srdd1.joinUniqFieldNames(srdd2, $"a" === $"a2", "inner")
    assertSrddSchemaEqual(result, "a: Integer; b: Double; C: String; a2: Integer; _c: String")
  }

  test("test smvJoinByKey") {
    val ssc   = sqlContext; import ssc.implicits._
    val srdd1 = dfFrom("a:Integer; b:Double; c:String", """1,2.0,hello;
         1,3.0,hello;
         2,10.0,hello2;
         2,11.0,hello3""")

    val srdd2 = dfFrom("a:Integer; c:String", """1,asdf;
         2,asdfg""")

    val result     = srdd1.smvJoinByKey(srdd2, Seq("a"), "inner")
    val fieldNames = result.columns
    assert(fieldNames === Seq("a", "b", "c", "_c"))
    assertUnorderedSeqEqual(result.collect.map(_.toString),
                            Seq("[1,2.0,hello,asdf]",
                                "[1,3.0,hello,asdf]",
                                "[2,10.0,hello2,asdfg]",
                                "[2,11.0,hello3,asdfg]"))
  }

  test("outer smvJoinByKey with single key column") {
    val df1 = dfFrom("a:Integer;b:String", """1,x1;2,y1;3,z1""")
    val df2 = dfFrom("a:Integer;b:String", """1,x2;4,w2;""")
    val res = df1.smvJoinByKey(df2, Seq("a"), SmvJoinType.Outer)
    assert(res.columns === Seq("a", "b", "_b"))
    assertUnorderedSeqEqual(res.collect.map(_.toString),
                            Seq("[1,x1,x2]", "[2,y1,null]", "[3,z1,null]", "[4,null,w2]"))
  }

  test("outer smvJoinByKey with multiple key columns") {
    val df1 = dfFrom("k1:Integer;k2:Integer;a:String", "1,1,x1;1,2,x2;2,1,x3;2,2,x4")
    val df2 = dfFrom("k1:Integer;k2:Integer;b:String", "1,1,y1;1,3,y2;3,1,y3;3,3,y4")
    val res = df1.smvJoinByKey(df2, Seq("k1", "k2"), SmvJoinType.Outer)
    assert(res.columns === Seq("k1", "k2", "a", "b"))
    assertUnorderedSeqEqual(res.collect.map(_.toString),
                            Seq("[1,1,x1,y1]",
                                "[1,2,x2,null]",
                                "[1,3,null,y2]",
                                "[2,1,x3,null]",
                                "[2,2,x4,null]",
                                "[3,1,null,y3]",
                                "[3,3,null,y4]"))
  }

  test("smvJoinByKey with underscore column name on the left table") {
    val df1 = dfFrom("a:String;_a:String", "a,1;a,2")
    val df2 = dfFrom("a:String;x:String", "a,f")

    val res = df1.smvJoinByKey(df2, Seq("a"), SmvJoinType.Inner)
    assert(res.columns === Seq("a", "_a", "x"))
    assertUnorderedSeqEqual(res.collect.map(_.toString), Seq("[a,1,f]", "[a,2,f]"))
  }

  test("smvJoinByKey with null-safe") {
    val df1 = dfFrom("a:String;y:String", "a,1;,2")
    val df2 = dfFrom("a:String;x:String", "a,f;,g")

    val res = df1.smvJoinByKey(df2, Seq("a"), SmvJoinType.Inner, isNullSafe = true)
    assert(res.columns === Seq("a", "y", "x"))
    assertUnorderedSeqEqual(res.collect.map(_.toString), Seq("[null,2,g]", "[a,1,f]"))
  }

  test("joinMultipleByKey") {
    val df1 = dfFrom("a:Integer;b:String", """1,x1;2,y1;3,z1""")
    val df2 = dfFrom("a:Integer;b:String", """1,x1;4,w2;""")
    val df3 = dfFrom("a:Integer;b:String", """1,x3;5,w3;""")

    val mtjoin = df1
      .smvJoinMultipleByKey(Seq("a"), SmvJoinType.Inner)
      .joinWith(df2, "_df2")
      .joinWith(df3, "_df3", SmvJoinType.Outer)

    val res = mtjoin.doJoin()

    assert(res.columns === Seq("a", "b", "b_df2", "b_df3"))
    assertSrddDataEqual(res,
                        """1,x1,x1,x3;
                          |5,null,null,w3""".stripMargin)

    val res2 = mtjoin.doJoin(true)
    assertSrddDataEqual(res2,
                        """1,x1;
                          |5,null""".stripMargin)
  }
}

class smvUnionTest extends SmvTestUtil {
  test("test smvUion") {
    val df  = dfFrom("a:Integer; b:Double; c:String", """1,2.0,hello;
         2,3.0,hello2""")
    val df2 = dfFrom("c:String; a:Integer; d:Double", """hello5,5,21.0;
         hello6,6,22.0""")

    val result = df.smvUnion(df2)
    assertUnorderedSeqEqual(result.collect.map(_.toString),
                            Seq("[1,2.0,hello,null]",
                                "[2,3.0,hello2,null]",
                                "[5,null,hello5,21.0]",
                                "[6,null,hello6,22.0]"))
  }
}

class dedupByKeyTest extends SmvTestUtil {
  test("test dedupByKey") {
    val df = dfFrom("a:Integer; b:Double; c:String", """1,2.0,hello;
         1,3.0,hello;
         2,10.0,hello2;
         2,11.0,hello3""")

    val result1 = df.dedupByKey("a")
    assertUnorderedSeqEqual(result1.collect.map(_.toString),
                            Seq("[1,2.0,hello]", "[2,10.0,hello2]"))

    val fieldNames1 = result1.schema.fieldNames
    assert(fieldNames1 === Seq("a", "b", "c"))

    val result2 = df.dedupByKey("a", "c")
    assertUnorderedSeqEqual(result2.collect.map(_.toString),
                            Seq("[1,2.0,hello]", "[2,10.0,hello2]", "[2,11.0,hello3]"))

    val fieldNames2 = result2.schema.fieldNames
    assert(fieldNames2 === Seq("a", "b", "c"))

  }

  test("test dedupByKey with nulls") {
    val df = dfFrom("a:Integer; b:Double; c:String", """1,,hello;
         1,3.0,hello1;
         2,10.0,;
         2,11.0,hello3""")

    val res = df.dedupByKey("a")
    assertUnorderedSeqEqual(res.collect.map(_.toString), Seq("[1,null,hello]", "[2,10.0,null]"))
  }

  test("test dedupByKeyWithOrder") {
    val ssc = sqlContext; import ssc.implicits._
    val df  = dfFrom("a:Integer; b:Double; c:String", """1,,hello;
         1,3.0,hello1;
         2,11.0,;
         2,10.0,hello3""")

    val res = df.dedupByKeyWithOrder($"a")($"b".desc)
    assertUnorderedSeqEqual(res.collect.map(_.toString), Seq("[1,3.0,hello1]", "[2,11.0,null]"))
  }

  test("test dedupByKeyWithOrder with timestamp") {
    val ssc = sqlContext; import ssc.implicits._
    val df  = dfFrom("a:Integer; b:Timestamp[yyyyMMdd]", """1,20150102;
         1,20140108;
         2,20130712;
         2,20150504""")

    val res = df.dedupByKeyWithOrder("a")($"b".desc)
    assertUnorderedSeqEqual(res.collect.map(_.toString),
                            Seq(
                              "[1,2015-01-02 00:00:00.0]",
                              "[2,2015-05-04 00:00:00.0]"
                            ))
  }
}

class smvOverlapCheckTest extends SmvTestUtil {
  test("test smvOverlapCheck") {
    val s1 = dfFrom("k: String", "a;b;c")
    val s2 = dfFrom("k: String", "a;b;c;d")
    val s3 = dfFrom("k: String", "c;d")

    val res = s1.smvOverlapCheck("k")(s2, s3)
    assertUnorderedSeqEqual(res.collect.map(_.toString),
                            Seq("[a,110]", "[b,110]", "[c,111]", "[d,011]"))

  }
}

class smvHashSampleTest extends SmvTestUtil {
  test("test smvHashSample") {
    val ssc = sqlContext; import ssc.implicits._
    val a   = dfFrom("key:String", "a;b;c;d;e;f;g;h;i;j;k")
    val res = a.union(a).smvHashSample($"key", 0.3)
    assertUnorderedSeqEqual(res.collect.map(_.toString),
                            Seq("[a]", "[g]", "[i]", "[a]", "[g]", "[i]"))
  }
}

class smvCoalesceTest extends SmvTestUtil {
  test("Test smvCoalesce") {
    val ssc = sqlContext; import ssc.implicits._
    val a   = dfFrom("key:String", "a;b;c;d;e;f;g;h;i;j;k")
    val res = a.coalesce(1)
    assertUnorderedSeqEqual(
      res.collect.map(_.toString),
      Seq("[a]", "[b]", "[c]", "[d]", "[e]", "[f]", "[g]", "[h]", "[i]", "[j]", "[k]"))
    assert(res.rdd.partitions.size === 1)
  }
}

class smvPipeCount extends SmvTestUtil {
  test("Test smvPipeCount") {
    val ssc     = sqlContext; import ssc.implicits._
    val a       = dfFrom("key:String", "a;b;c;d;e;f;g;h;i;j;k")
    val counter = sc.longAccumulator

    val n1 = a.smvPipeCount(counter).count
    val n2 = counter.value

    assert(n1 === n2)
  }
}

class EddDFHelperTest extends SmvTestUtil {
  test("smvConcatHist") {
    val a   = dfFrom("id:String; v:Integer", "a,1;b,1;a,1;a,2")
    val res = a._smvConcatHist(Seq("id", "v")).createReport()
    assert(res === """Histogram of id_v: String sort by Key
key                      count      Pct    cumCount   cumPct
a_1                          2   50.00%           2   50.00%
a_2                          1   25.00%           3   75.00%
b_1                          1   25.00%           4  100.00%
-------------------------------------------------""")
  }

  test("smvHist") {
    val a   = dfFrom("id:Date; v:Integer", "2010-01-01,1;2010-01-02,1;2010-01-01,1;2010-01-01,2")
    val res = a._smvFreqHist("id").createReport()
    assert(res === """Histogram of id: String sorted by Frequency
key                      count      Pct    cumCount   cumPct
2010-01-01                   3   75.00%           3   75.00%
2010-01-02                   1   25.00%           4  100.00%
-------------------------------------------------""")
  }

  test("smvFreqHist") {
    val a   = dfFrom("id:String; v:Integer", "a,1;b,1;a,1;a,2")
    val res = a._smvFreqHist("id").createReport()
    assert(res === """Histogram of id: String sorted by Frequency
key                      count      Pct    cumCount   cumPct
a                            3   75.00%           3   75.00%
b                            1   25.00%           4  100.00%
-------------------------------------------------""")
  }

  test("smvCountHist") {
    val a   = dfFrom("id:String; v:Integer", "a,1;b,1;a,1;a,2")
    val res = a._smvCountHist(Seq("id"), 1).createReport()
    assert(res === """Histogram of N_id: with BIN size 1.0
key                      count      Pct    cumCount   cumPct
1.0                          1   50.00%           1   50.00%
3.0                          1   50.00%           2  100.00%
-------------------------------------------------""")
  }

  test("smvBinHist") {
    val a   = dfFrom("id:String; v:Double", "a,100;b,1201;a,231;a,2")
    val res = a._smvBinHist("v" -> 100).createReport()
    assert(res === """Histogram of v: with BIN size 100.0
key                      count      Pct    cumCount   cumPct
0.0                          1   25.00%           1   25.00%
100.0                        1   25.00%           2   50.00%
200.0                        1   25.00%           3   75.00%
1200.0                       1   25.00%           4  100.00%
-------------------------------------------------""")
  }

  test("df.smvEddCompare(df2)") {
    val a = dfFrom("id:String; v:Double", "a,100;b,1201.5;a,231;a,2")
    val b = dfFrom("id:String; v:Double", "a,100;b,1201;a,231;a,2")

    val res = a._smvEddCompare(b, false)
    assert(res == """Not Match
not equal: v, stat, avg, Average
not equal: v, stat, std, Standard Deviation
not equal: v, stat, max, Max""")
  }
}

class OtherDFHelperTest extends SmvTestUtil {
  test("Test smvDiscoverPK") {
    val a          = dfFrom("a:String; b:String; c:String", """1,2,1;
         1,1,2;
         2,1,2;
         2,2,2""")
    val (res, cnt) = a.smvDiscoverPK()

    assertUnorderedSeqEqual(res, Seq("a", "b"))
    assert(cnt === 4)
  }
}
