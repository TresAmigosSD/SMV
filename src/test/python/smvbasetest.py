import unittest
from smv import Smv

import pyspark
from pyspark.context import SparkContext
from pyspark.sql import *

class SmvBaseTest(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.sc = SparkContext('local[*]', cls.__name__)
        cls.sqlContext = HiveContext(cls.sc)
        cls.smv = Smv(['-m', 'None'], cls.sqlContext)
        cls.app = cls.smv.app

    @classmethod
    def tearDownClass(cls):
        cls.sc.stop()

    @classmethod
    def createDF(cls, schema, data):
        return DataFrame(app.createDF(schema, data, False)._jdf, cls.sqlContext)
