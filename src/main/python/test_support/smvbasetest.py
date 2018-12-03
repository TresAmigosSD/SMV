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
from test_support.testconfig import TestConfig

import pyspark
from pyspark.context import SparkContext
from pyspark.sql import *
from smv.utils import smv_copy_array

import os, shutil, sys

class SmvBaseTest(unittest.TestCase):
    # DataDir value is deprecated. Use tmpDataDir instead
    DataDir = "./target/data"
    PytestDir = "./target/pytest"
    TestSrcDir = "./src/test/python"

    @classmethod
    def smvAppInitArgs(cls):
        return ['-m', 'None']

    @classmethod
    def setUpClass(cls):
        # Import needs to happen during EVERY setup to ensure that we are
        # using the most recently reloaded SmvApp
        from smv.smvapp import SmvApp

        cls.sparkSession = TestConfig.sparkSession()
        cls.sparkContext = TestConfig.sparkContext()
        cls.sparkContext.setLogLevel("ERROR")

        args = TestConfig.smv_args() + cls.smvAppInitArgs() + ['--data-dir', cls.tmpDataDir()]
        # The test's SmvApp must be set as the singleton for correct results of some tests
        # The original SmvApp (if any) will be restored when the test is torn down
        cls.smvApp = SmvApp.createInstance(args, cls.sparkSession)

        sys.path.append(cls.resourceTestDir())

        cls.mkTmpTestDir()

    @classmethod
    def tearDownClass(cls):
        # Import needs to happen during EVERY setup to ensure that we are
        # using the most recently reloaded SmvApp
        from smv.smvapp import SmvApp
        # Restore SmvApp singleton
        SmvApp.setInstance(TestConfig.originalSmvApp())
        sys.path.remove(cls.resourceTestDir())

    def setUp(self):
        """Patch for Python 2.6 without using unittest
        """
        from smv import SmvApp
        cls = self.__class__
        if not hasattr(cls, 'smvApp'):
            cls.sparkSession = TestConfig.sparkSession()
            cls.sparkContext = TestConfig.sparkContext()
            cls.sparkContext.setLogLevel("ERROR")

            args = TestConfig.smv_args() + cls.smvAppInitArgs() + ['--data-dir', cls.tmpDataDir()]
            cls.smvApp = SmvApp.createInstance(args, cls.sparkSession)

    @classmethod
    def createDF(cls, schema, data):
        return cls.smvApp.createDF(schema, data)

    @classmethod
    def df(cls, fqn, forceRun=False):
        return cls.smvApp.runModule(fqn, forceRun=forceRun)[0]

    @classmethod
    def load(cls, *fqn):
        return cls.smvApp.dsm.load(*fqn)

    def should_be_same(self, expected, result):
        """Asserts that the two dataframes contain the same data, ignoring order
        """

        # Since Python sort can't handle null values in DF, use DF's orderBy to sort
        def sort_collect(df):
            return df.coalesce(1).orderBy(*(df.columns)).collect()

        self.assertEqual(expected.columns, result.columns)
        self.assertEqual(sort_collect(expected), sort_collect(result))

    @classmethod
    def resourceTestDir(cls):
        """Directory where resources (like modules to run) for this test are expected."""
        return cls.TestSrcDir + "/" + cls.__module__

    @classmethod
    def tmpTestDir(cls):
        """Temporary directory for each test to put the files it creates. Automatically cleaned up."""
        return cls.PytestDir + "/" + cls.__name__

    @classmethod
    def tmpDataDir(cls):
        """Temporary directory for each test to put the data it creates. Automatically cleaned up."""
        return cls.tmpTestDir() + "/data"

    @classmethod
    def tmpInputDir(cls):
        """Temporary directory for each test to put the input files it creates. Automatically cleaned up."""
        return cls.tmpDataDir() + "/input"

    @classmethod
    def mkTmpTestDir(cls):
        shutil.rmtree(cls.tmpTestDir(), ignore_errors=True)
        os.makedirs(cls.tmpTestDir())

    def createTempInputFile(self, baseName, fileContents = "xxx"):
        """create a temp file in the input data dir with the given contents"""
        import os
        fullPath = self.tmpInputDir() + "/" + baseName
        directory = os.path.dirname(fullPath)
        if not os.path.exists(directory):
            os.makedirs(directory)

        f = open(fullPath, "w")
        f.write(fileContents)
        f.close()
