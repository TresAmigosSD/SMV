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

from test_support.smvbasetest import SmvBaseTest
from smv import SmvCsvFile

import pyspark
from pyspark.context import SparkContext
from pyspark.sql import SQLContext, HiveContext
from pyspark.sql.functions import col, struct, sum

class GroupedDataTest(SmvBaseTest):
    def test_smvRePartition(self):
        df = self.createDF("k:String; t:Integer; v:Double", "z,1,0.2;z,2,1.4;z,5,2.2;a,1,0.3;")
        res = df.smvGroupBy('k').smvRePartition(2).df
        self.assertEqual(res.rdd.getNumPartitions(), 2)
        self.should_be_same(df, res)

    def test_smvFillNullWithPrevValue(self):
        df = self.createDF("k:String; t:Integer; v:String", "a,1,;a,2,a;a,3,b;a,4,")
        res = df.smvGroupBy("k").smvFillNullWithPrevValue(col("t").asc())("v")
        expect = self.createDF("k:String; t:Integer; v:String",
              """a,1,;
                 a,2,a;
                 a,3,b;
                 a,4,b"""
        )
        self.should_be_same(expect, res)

    def test_smvPivot_smvPivotSum(self):
        df = self.createDF("id:String;month:String;product:String;count:Integer", "1,5/14,A,100;1,6/14,B,200;1,5/14,B,300")
        r1 = df.smvGroupBy('id').smvPivot([['month', 'product']],['count'],["5_14_A", "5_14_B", "6_14_A", "6_14_B"])
        r2 = df.smvGroupBy('id').smvPivotSum([['month', 'product']],['count'],["5_14_A", "5_14_B", "6_14_A", "6_14_B"])

        e1 = self.createDF("id: String;count_5_14_A: Integer;count_5_14_B: Integer;count_6_14_A: Integer;count_6_14_B: Integer",
                            """1,100,,,;
                               1,,,,200;
                               1,,300,,""")
        e2 = self.createDF("id: String;count_5_14_A: Long;count_5_14_B: Long;count_6_14_A: Long;count_6_14_B: Long",
                           "1,100,300,0,200")
        self.should_be_same(r1, e1)
        self.should_be_same(r2, e2)

    def test_smvPivotCoalesce(self):
        df = self.createDF("k:String; p:String; v:Integer", "a,c,1;a,d,2;a,e,;a,f,5")
        res = df.smvGroupBy("k").smvPivotCoalesce(
            [['p']],
            ['v'],
            ['c', 'd', 'e', 'f']
        )
        expect = self.createDF("k: String;v_c: Integer;v_d: Integer;v_e: Integer;v_f: Integer",
            "a,1,2,,5"
        )
        self.should_be_same(expect, res)

    def test_smvWithTimePanel(self):
        df = self.createDF("k:Integer; ts:String; v:Double",
            """1,20120101,1.5;
                1,20120301,4.5;
                1,20120701,7.5;
                1,20120501,2.45"""
            ).withColumn("ts", col('ts').smvStrToTimestamp("yyyyMMdd"))

        import smv.panel as p

        res = df.smvGroupBy('k').smvWithTimePanel(
            'ts', p.Month(2012,1), p.Month(2012,3)
        )

        expect = self.createDF("k: Integer;ts: String;v: Double;smvTime: String",
        """1,,,M201202;
            1,,,M201201;
            1,,,M201203;
            1,20120101,1.5,M201201;
            1,20120301,4.5,M201203""").withColumn("ts", col('ts').smvStrToTimestamp("yyyyMMdd"))

        self.should_be_same(expect, res)

    def test_smvTimePanelAgg(self):
        df = self.createDF("k:Integer; ts:String; v:Double",
            """1,20120101,1.5;
                1,20120301,4.5;
                1,20120701,7.5;
                1,20120501,2.45"""
            ).withColumn("ts", col('ts').smvStrToTimestamp("yyyyMMdd"))

        import smv.panel as p

        res = df.smvGroupBy('k').smvTimePanelAgg(
            'ts', p.Quarter(2012,1), p.Quarter(2012,2)
        )(
            sum('v').alias('v')
        )

        expect = self.createDF("k: Integer;smvTime: String;v: Double",
                """1,Q201201,6.0;
                    1,Q201202,2.45""")

        self.should_be_same(expect, res)

    def test_smvTimePanelAgg_with_Week(self):
        df = self.createDF("k:Integer; ts:String; v:Double",
                 "1,20120301,1.5;" +
                 "1,20120304,4.5;" +
                 "1,20120308,7.5;" +
                 "1,20120309,2.45"
             ).withColumn("ts", col('ts').smvStrToTimestamp("yyyyMMdd"))

        import smv.panel as p

        res = df.smvGroupBy('k').smvTimePanelAgg(
            'ts', p.Week(2012, 3, 1), p.Week(2012, 3, 10)
        )(
            sum('v').alias('v')
        )

        expect = self.createDF("k: Integer;smvTime: String;v: Double",
            """1,W20120305,9.95;
                1,W20120227,6.0""")

        self.should_be_same(res, expect)

    def test_smvPercentRank(self):
        df = self.createDF("id:String;v:Integer","a,1;a,;a,4;a,1;a,1;a,2;a,;a,5")
        res = df.smvGroupBy('id').smvPercentRank(['v'])

        exp = self.createDF("id: String;v: Integer;v_pctrnk: Double",
                            """a,,;
                            a,,;
                            a,1,0.0;
                            a,1,0.0;
                            a,1,0.0;
                            a,2,0.6;
                            a,4,0.7999999999999999;
                            a,5,1.0""")

        self.should_be_same(res, exp)

    def test_smvQuantile(self):
        df = self.createDF("id:String;v1:Integer;v2:Double","a,1,1.0;a,,2.0;a,4,;a,1,1.1;a,1,2.3;a,2,5.0;a,,3.1;a,5,1.2")
        res = df.smvGroupBy("id").smvQuantile(["v1", "v2"], 4)

        exp = self.createDF("id: String;v1: Integer;v2: Double;v1_quantile: Integer;v2_quantile: Integer",
                            """a,,2.0,,3;
                            a,,3.1,,4;
                            a,1,1.0,1,1;
                            a,1,1.1,1,1;
                            a,1,2.3,1,3;
                            a,2,5.0,3,4;
                            a,4,,4,;
                            a,5,1.2,4,2""")
        self.should_be_same(res, exp)
