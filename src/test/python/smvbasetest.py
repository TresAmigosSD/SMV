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
from smv import SmvApp

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
        cls.sparkSession = TestConfig.sparkSession()
        cls.sparkContext = TestConfig.sparkContext()
        cls.sparkContext.setLogLevel("ERROR")

        import random;
        callback_server_port = random.randint(20000, 65535)

        args = cls.smvAppInitArgs() + ['--cbs-port', str(callback_server_port), '--data-dir', cls.DataDir]
        cls.smvApp = SmvApp.createInstance(args, cls.sparkSession)

    def setUp(self):
        """Patch for Python 2.6 without using unittest
        """
        cls = self.__class__
        if not hasattr(cls, 'smvApp'):
            cls.sparkSession = TestConfig.sparkSession()
            cls.sparkContext = TestConfig.sparkContext()
            cls.sparkContext.setLogLevel("ERROR")

            import random;
            callback_server_port = random.randint(20000, 65535)

            args = cls.smvAppInitArgs() + ['--cbs-port', str(callback_server_port)]
            cls.smvApp = SmvApp.createInstance(args, cls.sparkSession)

    @classmethod
    def tearDownClass(cls):
        TestConfig.tearDown()
        del cls.sparkSession
        del cls.sparkContext

    @classmethod
    def createDF(cls, schema, data):
        return cls.smvApp.createDF(schema, data)

    @classmethod
    def df(cls, fqn):
        return cls.smvApp.runModule("mod:" + fqn)

    def should_be_same(self, expected, result):
        """Asserts that the two dataframes contain the same data, ignoring order
        """
        self.assertEqual(expected.columns, result.columns)
        self.assertEqual(sorted(expected.collect()), sorted(result.collect()))

    def createTempFile(self, baseName, fileContents = "xxx"):
        """create a temp file in the data dir with the given contents"""
        import os
        fullPath = self.DataDir + "/" + baseName
        directory = os.path.dirname(fullPath)
        if not os.path.exists(directory):
            os.makedirs(directory)

        f = open(fullPath, "w")
        f.write(fileContents)
        f.close()
