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
from runtests import TestConfig
from smv import smvPy

import pyspark
from pyspark.context import SparkContext
from pyspark.sql import *

class SmvBaseTest(unittest.TestCase):
    DataDir = "./target/data"

    @classmethod
    def smvAppInitArgs(cls):
        return ['-m', 'None']

    @classmethod
    def setUpClass(cls):
        cls.sparkContext = TestConfig.sparkContext()
        cls.sqlContext = TestConfig.sqlContext()
        cls.sparkContext.setLogLevel("ERROR")

        import random;
        callback_server_port = random.randint(20000, 65535)

        args = cls.smvAppInitArgs() + ['--cbs-port', str(callback_server_port), '--data-dir', cls.DataDir]
        smvPy.init(args, cls.sparkContext, cls.sqlContext)
        cls.smvPy = smvPy

    def setUp(self):
        """Patch for Python 2.6 without using unittest
        """
        cls = self.__class__
        if not hasattr(cls, 'smvPy'):
            cls.sparkContext = TestConfig.sparkContext()
            cls.sqlContext = TestConfig.sqlContext()
            cls.sparkContext.setLogLevel("ERROR")

            import random;
            callback_server_port = random.randint(20000, 65535)

            args = cls.smvAppInitArgs() + ['--cbs-port', str(callback_server_port)]
            smvPy.init(args, cls.sparkContext, cls.sqlContext)
            cls.smvPy = smvPy

    @classmethod
    def createDF(cls, schema, data):
        return cls.smvPy.createDF(schema, data)

    def should_be_same(self, expected, result):
        """Returns true if the two dataframes contain the same data, regardless of order
        """
        self.assertEqual(expected.columns, result.columns)
        self.assertEqual(expected.collect().sort(), result.collect().sort())
