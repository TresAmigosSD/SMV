import unittest
import runtests
from smv import Smv

import pyspark
from pyspark.context import SparkContext
from pyspark.sql import *

class SmvBaseTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        conf = runtests.TestConfig()
        cls.sqlContext = conf.sqlContext()
        cls.smv = Smv().init(['-m', 'None'], cls.sqlContext._sc, cls.sqlContext)
        cls.app = cls.smv.app

    def setUp(self):
        """Patch for Python 2.6 without using unittest
        """
        cls = self.__class__
        if not hasattr(cls, 'app'):
            conf = runtests.TestConfig()
            cls.sqlContext = conf.sqlContext()
            cls.smv = Smv().init(['-m', 'None'], cls.sqlContext._sc, cls.sqlContext)
            cls.app = cls.smv.app

    @classmethod
    def createDF(cls, schema, data):
        return DataFrame(cls.app.dfFrom(schema, data), cls.sqlContext)

    def should_be_same(self, left, right):
        """Returns true if the two dataframes are exactly the same, with both columns and rows in the same order.
        """
        self.assertEqual(left.collect(), right.collect())
