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

from test_support.smvbasetest import SmvBaseTest

import pyspark
from pyspark.context import SparkContext
from pyspark.sql import SQLContext, HiveContext
from pyspark.sql.functions import array, col

class ColumnHelperTest(SmvBaseTest):
    def test_smvGetColName(self):
        df = self.createDF("k:String; v:String;", "a,b;c,d;,")
        self.assertEqual(df.k.smvGetColName(), 'k')
        self.assertEqual(array(df.k, df.v).smvGetColName(), 'array(k,v)')

    def test_smvIsAllIn(self):
        df = self.createDF("k:String; v:String;", "a,b;c,d;,").select(array(col("k"), col("v")).alias("arr"))
        res = df.select(col("arr").smvIsAllIn("a", "b", "c").alias("isFound"))
        expected = self.createDF("isFound:Boolean", "true;false;false")
        self.should_be_same(expected, res)

    def test_smvIsAnyIn(self):
        df = self.createDF("k:String; v:String;", "a,b;c,d;,").select(array(col("k"), col("v")).alias("arr"))
        res = df.select(col("arr").smvIsAnyIn("a", "z").alias("isFound"))
        expected = self.createDF("isFound:Boolean", "true;false;false")
        self.should_be_same(expected,res)

    def test_smvDateTimeFunctions(self):
        df = self.createDF("k:Timestamp[yyyyMMdd]; v:String;", "20190101,a;,b")
        res = df.select(col("k").smvYear(), col("k").smvMonth(), col("k").smvQuarter(), col("k").smvDayOfMonth(), col("k").smvDayOfWeek(), col("k").smvHour())
        expected = self.createDF("SmvYear(k): Integer; SmvMonth(k): Integer; SmvQuarter(k): Integer; SmvDayOfMonth(k): Integer; SmvDayOfWeek(k): Integer; SmvHour(k): Integer", "2019,1,1,1,3,0;" + ",,,,,")

        if sys.version < '3':
            self.should_be_same(expected, res)
        else:
            # Python 3 is a bit picky about null ordering
            self.assertEquals(expected.columns, res.columns)
            a = expected.collect()
            b = res.collect()
            try: a.sort()
            except TypeError: pass
            try: b.sort()
            except TypeError: pass
            self.assertEqual(a, b)

    def test_smvPlusDateTime(self):
        df = self.createDF("t:Timestamp[yyyyMMdd]", "19760131;20120229")
        r1 = df.select(col("t").smvPlusDays(-10).alias("ts"))
        r2 = df.select(col("t").smvPlusMonths(1).alias("ts"))
        r3 = df.select(col("t").smvPlusWeeks(3).alias("ts"))
        r4 = df.select(col("t").smvPlusYears(2).alias("ts"))
        r5 = df.select(col("t").smvPlusYears(4).alias("ts"))

        s = "ts: Timestamp[yyyy-MM-dd hh:mm:ss.S]"
        e1 = self.createDF(
            s,
            "1976-01-21 00:00:00.0;" +
            "2012-02-19 00:00:00.0")
        e2 = self.createDF(
            s,
            "1976-02-29 00:00:00.0;" +
            "2012-03-29 00:00:00.0")
        e3 = self.createDF(
            s,
            "1976-02-21 00:00:00.0;" +
            "2012-03-21 00:00:00.0")
        e4 = self.createDF(
            s,
            "1978-01-31 00:00:00.0;" +
            "2014-02-28 00:00:00.0")
        e5 = self.createDF(
            s,
            "1980-01-31 00:00:00.0;" +
            "2016-02-29 00:00:00.0")

        self.should_be_same(e1, r1)
        self.should_be_same(e2, r2)
        self.should_be_same(e3, r3)
        self.should_be_same(e4, r4)
        self.should_be_same(e5, r5)

    def test_smvPlusDateTime_Column(self):
        df = self.createDF("t:Timestamp[yyyyMMdd];toadd:Integer", "19760131,10;20120229,32;19070101,")

        r1 = df.select(col("t").smvPlusDays(col("toadd")).alias('ts'))
        r2 = df.select(col("t").smvPlusWeeks(col("toadd")).alias('ts'))
        r3 = df.select(col("t").smvPlusMonths(col("toadd")).alias('ts'))
        r4 = df.select(col("t").smvPlusYears(col("toadd")).alias('ts'))

        s = "ts: Timestamp[yyyy-MM-dd hh:mm:ss.S]"
        e1 = self.createDF(
            s,
            """1976-02-10 00:00:00.0;
               2012-04-01 00:00:00.0;
            """
            )
        e2 = self.createDF(
            s,
            """1976-04-10 00:00:00.0;
               2012-10-10 00:00:00.0;
            """
            )
        e3 = self.createDF(
            s,
            """1976-11-30 00:00:00.0;
               2014-10-29 00:00:00.0;
            """
            )
        e4 = self.createDF(
            s,
            """1986-01-31 00:00:00.0;
               2044-02-29 00:00:00.0;
            """
            )
        self.should_be_same(e1, r1)
        self.should_be_same(e2, r2)
        self.should_be_same(e3, r3)
        self.should_be_same(e4, r4)

    def test_smvDayMonth70(self):
        df = self.createDF("t:Timestamp[yyyyMMdd]", "19760131;20120229")
        r1 = df.select(col("t").smvDay70().alias("t_day70"))
        r2 = df.select(col("t").smvMonth70().alias("t_month70"))

        e1 = self.createDF("t_day70: Integer", "2221;15399")
        e2 = self.createDF("t_month70: Integer", "72;505")

        self.should_be_same(e1, r1)
        self.should_be_same(e2, r2)

    def test_smvTime_helpers(self):
        df = self.createDF("smvTime:String", "D20120302;Q201203;M201203")
        res = df.withColumn('type',
            df.smvTime.smvTimeToType()
        ).withColumn('index',
            df.smvTime.smvTimeToIndex()
        ).withColumn('label',
            df.smvTime.smvTimeToLabel()
        )

        res.smvDumpDF()
