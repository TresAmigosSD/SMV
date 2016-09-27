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
        cls.smv = Smv(['-m', 'None'], cls.sqlContext)
        cls.app = cls.smv.app

    @classmethod
    def createDF(cls, schema, data):
        return DataFrame(cls.app.dfFrom(schema, data), cls.sqlContext)
