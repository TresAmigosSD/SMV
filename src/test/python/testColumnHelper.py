import unittest

from smvbasetest import SmvBaseTest

import pyspark
from pyspark.context import SparkContext
from pyspark.sql import SQLContext, HiveContext
from pyspark.sql.functions import array, col

class ColumnHelperTest(SmvBaseTest):
    def test_smvIsAllIn(self):
        df = self.createDF("k:String; v:String;", "a,b;c,d;,").select(array(col("k"), col("v")).alias("arr"))
        res = df.select(col("arr").smvIsAllIn("a", "b", "c").alias("isFound"))
        expected = self.createDF("isFound:Boolean", "true;false;false")
        self.should_be_same(expected, res)

    def test_smvMonth(self):
        return "TODO implement"
