from unittest import *

from pyspark import SparkContext
from pyspark.sql import HiveContext
from smv import Smv

# shared spark and sql context
class TestConfig(object):
    @classmethod
    def sparkContext(cls):
        if not hasattr(cls, 'sc'):
            cls.sc = SparkContext(appName="SMV Python Tests")
        return cls.sc

    @classmethod
    def sqlContext(cls):
        if not hasattr(cls, 'sqlc'):
            cls.sqlc = HiveContext(cls.sparkContext())
        return cls.sqlc

if __name__ == "__main__":
    conf = TestConfig()

    suite = TestLoader().discover("./src/test/python")
    result = TextTestRunner(verbosity=2).run(suite)
    print("result is ", result)
    exit(len(result.errors) + len(result.failures))
