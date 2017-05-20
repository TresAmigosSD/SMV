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
from pyspark.sql.functions import col, struct

class GroupedDataTest(SmvBaseTest):
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
