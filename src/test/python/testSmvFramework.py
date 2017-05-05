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

from test_support.smvbasetest import SmvBaseTest
from smv import *
from smv.dqm import *

import pyspark
from pyspark.context import SparkContext
from pyspark.sql import SQLContext, HiveContext
from pyspark.sql.functions import col, lit
from py4j.protocol import Py4JJavaError
from smvframework.stage.modules import D1, D2, D3, D4


class SmvFrameworkTest(SmvBaseTest):
    @classmethod
    def smvAppInitArgs(cls):
        return ['--smv-props', 'smv.stages=smvframework.stage']

    def _escapeRegex(self, s):
        import re
        return re.sub(r"([\[\]\(\)])", r"\\\1", s)

    def test_SmvCsvStringData(self):
        fqn = D1.fqn()
        df = self.df(fqn)
        expect = self.createDF("a:String;b:Integer", "x,10;y,1")
        self.should_be_same(expect, df)

    def test_SmvPyMultiCsvFiles(self):
        self.createTempFile("input/test3/f1", "col1\na\n")
        self.createTempFile("input/test3/f2", "col1\nb\n")
        self.createTempFile("input/test3.schema", "col1: String\n")

        fqn = D2.fqn()
        df = self.df(fqn)
        exp = self.createDF("col1: String", "a;b")
        self.should_be_same(df, exp)

    def test_SmvDQM(self):
        fqn = D3.fqn()

        msg =""": org.tresamigos.smv.SmvDqmValidationError: {
  "passed":false,
  "errorMessages": [
    {"FailTotalRuleCountPolicy(2)":"true"},
    {"FailTotalFixCountPolicy(1)":"false"}
  ],
  "checkLog": [
    "Rule: b_lt_03, total count: 1",
    "org.tresamigos.smv.dqm.DQMRuleError: b_lt_03 @FIELDS: b=0.5",
    "Fix: a_lt_1_fix, total count: 1"
  ]
}"""

        with self.assertRaisesRegexp(Py4JJavaError, self._escapeRegex(msg)):
            df = self.df(fqn)
            df.smvDumpDF()

    #TODO: add other SmvPyDataSet unittests

class SmvRunConfigTest1(SmvBaseTest):

    @classmethod
    def smvAppInitArgs(cls):
        return ['--smv-props', 'smv.config.s=s1', 'smv.stages=smvframework.stage',
                '-m', "None"]

    def test_SmvCsvStringData_with_SmvRunConfig(self):
        fqn = D4.fqn()
        df = self.df(fqn)
        expect = self.createDF("a:String;b:Integer", "a,10;b,1")
        self.should_be_same(expect, df)

class SmvRunConfigTest2(SmvBaseTest):

    @classmethod
    def smvAppInitArgs(cls):
        return ['--smv-props', 'smv.config.s=s2', 'smv.stages=smvframework.stage',
                '-m', "None"]

    def test_SmvCsvStringData_with_SmvRunConfig(self):
        fqn = D4.fqn()
        df = self.df(fqn)
        expect = self.createDF("a:String;b:Integer", "X,100;Y,200")
        self.should_be_same(expect, df)
