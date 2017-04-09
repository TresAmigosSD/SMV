#
# This file is licensed under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import unittest

from smvbasetest import SmvBaseTest
from smv import SmvApp, SmvPyCsvFile

from smv.helpers import DataFrameHelper as dfhelper

import pyspark
from pyspark.context import SparkContext
from pyspark.sql import SQLContext, HiveContext
from pyspark.sql.functions import col, struct

class T(SmvPyCsvFile):
    @classmethod
    def path(cls):
        return "./target/python-test-export-csv.csv"
    def csvAttr(self):
        return self.defaultCsvWithHeader()

class DfHelperTest(SmvBaseTest):
    def test_smvGroupBy(self):
        return "TODO implement"

    def test_smvHashSample_with_string(self):
        df = self.createDF("k:String", "a;b;c;d;e;f;g;h;i;j;k")
        r1 = df.unionAll(df).smvHashSample('k', 0.3)
        expect = self.createDF("k:String", "a;g;i;a;g;i")
        self.should_be_same(expect, r1)

    def test_smvHashSample_with_column(self):
        df = self.createDF("k:String", "a;b;c;d;e;f;g;h;i;j;k")
        r1 = df.unionAll(df).smvHashSample(col('k'), 0.3)
        expect = self.createDF("k:String", "a;g;i;a;g;i")
        self.should_be_same(expect, r1)

    def test_smvDedupByKey_with_string(self):
        schema = "a:Integer; b:Double; c:String"
        df = self.createDF(
            schema,
            """1,2.0,hello;
            1,3.0,hello;
            2,10.0,hello2;
            2,11.0,hello3"""
        )
        r1 = df.smvDedupByKey("a")
        expect = self.createDF(
            schema,
            """1,2.0,hello;
            2,10.0,hello2"""
        )
        self.should_be_same(expect, r1)

    def test_smvDedupByKey_with_column(self):
        schema = "a:Integer; b:Double; c:String"
        df = self.createDF(
            schema,
            """1,2.0,hello;
            1,3.0,hello;
            2,10.0,hello2;
            2,11.0,hello3"""
        )
        r1 = df.smvDedupByKey(col("a"))
        expect = self.createDF(
            schema,
            """1,2.0,hello;
            2,10.0,hello2"""
        )
        self.should_be_same(expect, r1)

    def test_smvDedupByKeyWithOrder_with_string(self):
        schema = "a:Integer; b:Double; c:String"
        df = self.createDF(
            schema,
            """1,2.0,hello;
            1,3.0,hello;
            2,10.0,hello2;
            2,11.0,hello3"""
        )
        r1 = df.smvDedupByKeyWithOrder("a")(col("b").desc())
        expect = self.createDF(
            schema,
            """1,3.0,hello;
            2,11.0,hello3"""
        )
        self.should_be_same(expect, r1)

    def test_smvDedupByKeyWithOrder_with_column(self):
        schema = "a:Integer; b:Double; c:String"
        df = self.createDF(
            schema,
            """1,2.0,hello;
            1,3.0,hello;
            2,10.0,hello2;
            2,11.0,hello3"""
        )
        r1 = df.smvDedupByKeyWithOrder(col("a"))(col("b").desc())
        expect = self.createDF(
            schema,
            """1,3.0,hello;
            2,11.0,hello3"""
        )
        self.should_be_same(expect, r1)

    def test_smvExpandStruct(self):
        schema = "id:String;a:Double;b:Double"
        df1 = self.createDF(schema, "a,1.0,10.0;a,2.0,20.0;b,3.0,30.0")
        df2 = df1.select(col("id"), struct("a", "b").alias("c"))
        res = df2.smvExpandStruct("c")
        expect = self.createDF(schema, "a,1.0,10.0;a,2.0,20.0;b,3.0,30.0")
        self.should_be_same(expect, res)

    def test_smvExportCsv(self):
        df = self.createDF("k:String;v:Integer", "a,1;b,2")
        df.smvExportCsv(T.path())

        res = self.smvApp.runModule("mod:" + self.__module__ + ".T")
        self.should_be_same(df, res)

    def test_smvJoinByKey(self):
        df1 = self.createDF(
            "a:Integer; b:Double; c:String",
            """1,2.0,hello;
            1,3.0,hello;
            2,10.0,hello2;
            2,11.0,hello3"""
        )
        df2 = self.createDF("a:Integer; c:String", """1,asdf;2,asdfg""")
        res = df1.smvJoinByKey(df2, ['a'], "inner")
        expect = self.createDF(
            "a:Integer;b:Double;c:String;_c:String",
            "1,2.0,hello,asdf;1,3.0,hello,asdf;2,10.0,hello2,asdfg;2,11.0,hello3,asdfg"
        )
        self.should_be_same(expect, res)

    def test_smvJoinMultipleByKey(self):
        df1 = self.createDF("a:Integer;b:String", """1,x1;2,y1;3,z1""")
        df2 = self.createDF("a:Integer;b:String", """1,x1;4,w2;""")
        df3 = self.createDF("a:Integer;b:String", """1,x3;5,w3;""")

        mj = df1.smvJoinMultipleByKey(['a'], 'inner').joinWith(df2, '_df2').joinWith(df3, '_df3', 'outer')
        r1 = mj.doJoin()

        self.assertEquals(r1.columns, ['a', 'b', 'b_df2', 'b_df3'])
        self.should_be_same(r1, self.createDF(
            "a:Integer;b:String;b_df2:String;b_df3:String",
            "1,x1,x1,x3;5,,,w3"))

        r2 = mj.doJoin(True)
        self.should_be_same(r2, self.createDF(
            "a:Integer;b:String",
            "1,x1;5,"))

        r3 = df1.smvJoinMultipleByKey(['a'], 'leftouter').joinWith(df2, "_df2").doJoin()
        self.should_be_same(r3, self.createDF(
            "a:Integer;b:String;b_df2:String",
            """1,x1,x1;
            2,y1,;
            3,z1,"""
        ))

    def test_smvUnion(self):
        schema       = "a:Integer; b:Double; c:String"
        schema2      = "c:String; a:Integer; d:Double"
        schemaExpect = "a:Integer; b:Double; c:String; d:Double"

        df = self.createDF(
            schema,
            """1,2.0,hello;
               2,3.0,hello2"""
        )
        df2 = self.createDF(
            schema2,
            """hello5,5,21.0;
               hello6,6,22.0"""
        )
        result = df.smvUnion(df2)
        expect = self.createDF(
            schemaExpect,
            """1,2.0,hello,;
            2,3.0,hello2,;
            5,,hello5,21.0;
            6,,hello6,22.0;"""
        )
        self.should_be_same(expect, result)

    def test_smvRenameField(self):
        schema       = "a:Integer; b:Double; c:String"
        schemaExpect = "aa:Integer; b:Double; cc:String"
        df = self.createDF(
            schema,
            """1,2.0,hello"""
        )
        result = df.smvRenameField(("a", "aa"), ("c", "cc"))
        expect = self.createDF(
            schemaExpect,
            """1,2.0,hello"""
        )

        fieldNames = result.columns
        self.assertEqual(fieldNames, ['aa', 'b', 'cc'])
        self.should_be_same(expect, result)

    def test_smvUnpivot(self):
        df = self.createDF("id:String; X:String; Y:String; Z:String",
            """1,A,B,C; 2,D,E,F""")
        res = df.smvUnpivot("X", "Y", "Z")
        expect = self.createDF("id: String;column: String;value: String",
            """1,X,A;
            1,Y,B;
            1,Z,C;
            2,X,D;
            2,Y,E;
            2,Z,F""")
        self.should_be_same(expect, res)

    def test_smvUnpivotRegex(self):
        df = self.createDF("id:Integer; A_1:String; A_2:String; B_1:String; B_2:String",
                """1,1_a_1,1_a_2,1_b_1,1_b_2;
                   2,2_a_1,2_a_2,2_b_1,2_b_2
                """)
        res = df.smvUnpivotRegex( ["A_1", "A_2", "B_1", "B_2"], "(.*)_(.*)", "index" )
        expect = self.createDF("id: Integer; index:String; A:String; B:String",
                """1,1,1_a_1,1_b_1;
                   1,2,1_a_2,1_b_2;
                   2,1,2_a_1,2_b_1;
                   2,2,2_a_2,2_b_2
                """)
        self.should_be_same(expect, res)


    def test_smvSelectMinus_with_string(self):
        schema = "k:String;v1:Integer;v2:Integer"
        df = self.createDF(schema, "a,1,2;b,2,3")
        r1 = df.smvSelectMinus("v1")
        expect = self.createDF("k:String;v2:Integer", "a,2;b,3")
        self.should_be_same(expect, r1)

    def test_smvSelectMinus_with_column(self):
        schema = "k:String;v1:Integer;v2:Integer"
        df = self.createDF(schema, "a,1,2;b,2,3")
        r1 = df.smvSelectMinus(col("v1"))
        expect = self.createDF("k:String;v2:Integer", "a,2;b,3")
        self.should_be_same(expect, r1)

    def test_smvSelectPlus(self):
        df = self.createDF("k:String;v:Integer", "a,1;b,2")
        r1 = df.smvSelectPlus((col('v') + 1).alias("v2"))
        expect = self.createDF("k:String;v:Integer;v2:Integer", "a,1,2;b,2,3")
        self.should_be_same(expect, r1)

    def test_smvDesc(self):
        df = self.createDF("a:String", "a")
        res = df.smvDesc(("a", "this is col a"))
        self.assertEqual(res.schema.fields[0].metadata["smvDesc"], "this is col a")

    def test_smvGetDesc(self):
        df = self.createDF("a:String", "a")
        res = df.smvDesc(("a", "this is col a"))
        self.assertEqual(res.smvGetDesc("a"), "this is col a")
        self.assertEqual(res.smvGetDesc(), [("a", "this is col a")])

    def test_smvRemoveDesc(self):
        df = self.createDF("a:String", "a")
        res = df.smvDesc(("a", "this is col a")).smvRemoveDesc("a")
        self.assertEqual(res.smvGetDesc("a"), "")

    def test_smvDescFromDF(self):
        df = self.createDF("a:String;b:Integer", "a,1")
        desc = self.createDF("c:String;desc:String", "a,this is col a from a df;b,this is b")
        res = df.smvDescFromDF(desc)
        self.assertEqual(res.smvGetDesc("b"), "this is b")

class ShellDfHelperTest(SmvBaseTest):
    def test_smvEdd(self):
        df = self.createDF("k:String;v:Integer", "a,1;b,2")
        res = dfhelper(df)._smvEdd()
        self.assertEqual(res, """k                    Non-Null Count         2
k                    Null Count             0
k                    Min Length             1
k                    Max Length             1
k                    Approx Distinct Count  2
v                    Non-Null Count         2
v                    Null Count             0
v                    Average                1.5
v                    Standard Deviation     0.7071067811865476
v                    Min                    1.0
v                    Max                    2.0""")

    def test_smvHist(self):
        df = self.createDF("k:String;v:Integer", "a,1;b,2")
        res = dfhelper(df)._smvHist("k")
        self.assertEqual(res, """Histogram of k: String sort by Key
key                      count      Pct    cumCount   cumPct
a                            1   50.00%           1   50.00%
b                            1   50.00%           2  100.00%
-------------------------------------------------""")

    def test_smvConcatHist(self):
        df = self.createDF("k:String;v:String", "a,1;b,2")
        res = dfhelper(df)._smvConcatHist("k", "v")
        self.assertEqual(res, """Histogram of k_v: String sort by Key
key                      count      Pct    cumCount   cumPct
a_1                          1   50.00%           1   50.00%
b_2                          1   50.00%           2  100.00%
-------------------------------------------------""")

    def test_smvFreqHist(self):
        import smv.helpers as smv
        df = self.createDF("k:String;v:String", "a,1;b,2;a,3")
        res = dfhelper(df)._smvFreqHist("k")
        self.assertEqual(res, """Histogram of k: String sorted by Frequency
key                      count      Pct    cumCount   cumPct
a                            2   66.67%           2   66.67%
b                            1   33.33%           3  100.00%
-------------------------------------------------""")

    def test_smvCountHist(self):
        import smv.helpers as smv
        df = self.createDF("k:String;v:String", "a,1;b,2;a,3")
        res = dfhelper(df)._smvCountHist(["k"], 1)
        self.assertEqual(res, """Histogram of N_k: with BIN size 1.0
key                      count      Pct    cumCount   cumPct
1.0                          1   50.00%           1   50.00%
2.0                          1   50.00%           2  100.00%
-------------------------------------------------""")

    def test_smvBinHist(self):
        import smv.helpers as smv
        df = self.createDF("k:String;v:Integer", "a,10;b,200;a,30")
        res = dfhelper(df)._smvBinHist(("v", 100))
        self.assertEqual(res, """Histogram of v: with BIN size 100.0
key                      count      Pct    cumCount   cumPct
0.0                          2   66.67%           2   66.67%
200.0                        1   33.33%           3  100.00%
-------------------------------------------------""")

    def test_smvOverlapCheck(self):
        s1 = self.createDF("k: String", "a;b;c")
        s2 = self.createDF("k: String", "a;b;c;d")
        s3 = self.createDF("k: String", "c;d")

        res = s1.smvOverlapCheck("k")(s2, s3)
        exp = self.createDF("k: String;flag: String", "a,110;b,110;c,111;d,011")
        self.should_be_same(res, exp)
