import unittest

from smvbasetest import SmvBaseTest

import pyspark
from pyspark.context import SparkContext
from pyspark.sql import SQLContext, HiveContext
from pyspark.sql.functions import col

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

    def test_dedupByKey(self):
        schema = "a:Integer; b:Double; c:String"
        df = self.createDF(
            schema,
            """1,2.0,hello;
            1,3.0,hello;
            2,10.0,hello2;
            2,11.0,hello3"""
        )
        r1 = df.smvDedupByKey("a")
        # TODO: smvDedupByKey is reordering the output. Need to assert unordered like Scala?
        expect = self.createDF(
            schema,
            """2,10.0,hello2;
            1,2.0,hello
            """
        )
        self.should_be_same(expect, r1)

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

class ColumnHelperTest(unittest.TestCase):
    def test_smvMonth(self):
        return "TODO implement"
