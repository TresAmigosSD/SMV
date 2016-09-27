import unittest

from smvbasetest import SmvBaseTest

import pyspark
from pyspark.context import SparkContext
from pyspark.sql import SQLContext, HiveContext

class DfHelperTest(SmvBaseTest):
    def test_selectPlus(self):
        df = self.createDF("k:String;v:Integer", "a,1;b,2;c,3")
        print()
        df.show()

    def test_smvGroupBy(self):
        df = self.createDF("k:String;v:Integer", "a,1;b,2;c,3")
        print()
        df.printSchema()

class ColumnHelperTest(unittest.TestCase):
    def test_smvMonth(self):
        self.fail("oh no!")
