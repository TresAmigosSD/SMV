import unittest
from runtests import TestConfig
from smv import SmvApp

import pyspark
from pyspark.context import SparkContext
from pyspark.sql import *

class SmvBaseTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.sparkContext = TestConfig.sparkContext()
        cls.sqlContext = TestConfig.sqlContext()
        cls.sparkContext.setLogLevel("ERROR")
        cls.smv = SmvApp.init(['-m', 'None'], cls.sparkContext, cls.sqlContext)
        cls.app = cls.smv.app

    def setUp(self):
        """Patch for Python 2.6 without using unittest
        """
        cls = self.__class__
        if not hasattr(cls, 'app'):
            cls.sparkContext = TestConfig.sparkContext()
            cls.sqlContext = TestConfig.sqlContext()
            cls.sparkContext.setLogLevel("ERROR")
            cls.smv = SmvApp.init(['-m', 'None'], cls.sparkContext, cls.sqlContext)
            cls.app = cls.smv.app

    @classmethod
    def createDF(cls, schema, data):
        return DataFrame(cls.app.dfFrom(schema, data), cls.sqlContext)

    def should_be_same(self, expected, result):
        """Returns true if the two dataframes contain the same data, regardless of order
        """
        self.assertEqual(expected.columns, result.columns)
        self.assertEqual(expected.collect().sort(), result.collect().sort())
