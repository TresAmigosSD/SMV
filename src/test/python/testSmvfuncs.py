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
import sys

import pyspark
from pyspark.context import SparkContext
from pyspark.sql import SQLContext, HiveContext
from pyspark.sql.functions import *

from test_support.smvbasetest import SmvBaseTest
from smv.functions import *

class SmvfuncsTest(SmvBaseTest):
    def test_smvFirst(self):
        df = self.createDF("k:String; t:Integer; v:Double", "z,1,;z,2,1.4;z,5,2.2;a,1,0.3;")

        res = df.groupBy("k").agg(
            smvFirst(df.t, True).alias("first_t"), # use smvFirst instead of Spark's first to test the alternative form also
            smvFirst(df.v, True).alias("first_v"),
            smvFirst(df.v).alias("smvFirst_v")
        )

        exp = self.createDF("k: String;first_t: Integer;first_v: Double;smvFirst_v: Double",
            "a,1,0.3,0.3;" + \
            "z,1,1.4,")

        self.should_be_same(res, exp)

    def test_distMetric(self):
        df = self.createDF("s1:String; s2:String",
            ",ads;" +\
            "asdfg,asdfg;" +\
            "asdfghj,asdfhgj"
        )

        trunc = lambda c: pyspark.sql.functions.round(c,2)
        res = df.select(
            df.s1, df.s2,
            trunc(nGram2(df.s1, df.s2)).alias("nGram2"),
            trunc(nGram3(df.s1, df.s2)).alias("nGram3"),
            trunc(diceSorensen(df.s1, df.s2)).alias("diceSorensen"),
            trunc(normlevenshtein(df.s1, df.s2)).alias("normlevenshtein"),
            trunc(jaroWinkler(df.s1, df.s2)).alias("jaroWinkler")
        )

        exp = self.createDF("s1: String;s2: String;nGram2: Float;nGram3: Float;diceSorensen: Float;normlevenshtein: Float;jaroWinkler: Float",
            ",ads,,,,,;" + \
            "asdfg,asdfg,1.0,1.0,1.0,1.0,1.0;" + \
            "asdfghj,asdfhgj,0.5,0.4,0.5,0.71,0.97")

        self.should_be_same(res, exp)

    def test_smvCreateLookup(self):
        from pyspark.sql.types import StringType
        df = self.createDF("k:String;v:Integer", "a,1;b,2;,3;c,4")
        map_key = smvCreateLookUp({"a":"AA", "b":"BB"}, "__", StringType())
        res = df.withColumn("mapped", map_key(df.k))
        exp = self.createDF("k: String;v: Integer;mapped: String",
            "a,1,AA;" +
            "b,2,BB;" +
            ",3,__;" +
            "c,4,__")
        self.should_be_same(res, exp)

    def test_smvArrayCat(self):
        df = self.createDF("nullflag:Integer;a:String;b:String", "0,a,1;1,,;0,b,2")
        df2 = df.select(when(df.nullflag == 0, array(df.a, df.b))\
            .otherwise(lit(None)).alias("aa"))
        res = df2.select(smvArrayCat("_", df2.aa).alias("aaCat"))
        exp = self.createDF("aaCat: String", "a_1;; b_2")
        self.should_be_same(res, exp)

    def test_smvCollectSet(self):
        from pyspark.sql.types import StringType
        df = self.createDF("a:String", "a;b;a;c;a")
        res = df.agg(smvArrayCat("_", sort_array(smvCollectSet(df.a, StringType()))).alias("aa"))
        exp = self.createDF("aa: String", "a_b_c")
        self.should_be_same(res, exp)

    def test_smvStrCat(self):
        df = self.createDF("a:String;b:Integer", "a,1;b,2;c,")
        res = df.select(smvStrCat("_", df.a, df.b).alias("a_b"))
        exp = self.createDF("a_b: String", "a_1;b_2;c_")
        self.should_be_same(res, exp)

    def test_smvHashKey(self):
        df = self.createDF("a:String;b:Integer", "a,1;,;c,;a,1")
        res = df.select(smvHashKey("pre_", df.a, df.b).alias("key"))
        exp = self.createDF("key: String",
            "pre_8a8bb7cd343aa2ad99b7d762030857a2;" +
            "pre_;" +
            "pre_4a8a08f09d37b73795649038408b5f33;" +
            "pre_8a8bb7cd343aa2ad99b7d762030857a2"
        )
        self.should_be_same(res, exp)
