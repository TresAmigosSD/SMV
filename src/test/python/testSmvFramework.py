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

from smvbasetest import SmvBaseTest
from smv import *
from smv.dqm import *

import pyspark
from pyspark.context import SparkContext
from pyspark.sql import SQLContext, HiveContext
from pyspark.sql.functions import col, lit
from py4j.protocol import Py4JJavaError


class D1(SmvPyCsvStringData):
    def schemaStr(self):
        return "a:String;b:Integer"
    def dataStr(self):
        return "x,10;y,1"

class D2(SmvPyMultiCsvFiles):
    def dir(self):
        return "test3"

class D3(SmvPyCsvStringData):
    def schemaStr(self):
        return "a:Integer;b:Double"
    def dataStr(self):
        return "1,0.3;0,0.2;3,0.5"
    def dqm(self):
        return SmvDQM().add(
            DQMRule(col("b") < 0.4 , "b_lt_03")).add(
            DQMFix(col("a") < 1, lit(1).alias("a"), "a_lt_1_fix")).add(
            FailTotalRuleCountPolicy(2)).add(
            FailTotalFixCountPolicy(1))

class SmvFrameworkTest(SmvBaseTest):
    def _escapeRegex(self, s):
        import re
        return re.sub(r"([\[\]\(\)])", r"\\\1", s)

    def test_SmvCsvStringData(self):
        fqn = self.__module__ + ".D1"
        df = self.df(fqn)
        expect = self.createDF("a:String;b:Integer", "x,10;y,1")
        self.should_be_same(expect, df)

    def test_SmvPyMultiCsvFiles(self):
        self.createTempFile("input/test3/f1", "col1\na\n")
        self.createTempFile("input/test3/f2", "col1\nb\n")
        self.createTempFile("input/test3.schema", "col1: String\n")

        fqn = self.__module__ + ".D2"
        df = self.df(fqn)
        exp = self.createDF("col1: String", "a;b")
        self.should_be_same(df, exp)

    def test_SmvDQM(self):
        fqn = self.__module__ + ".D3"

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

class D4(SmvPyCsvStringData, SmvRunConfig):
    def schemaStr(self):
        return "a:String;b:Integer"
    def dataStr(self):
        if(self.smvGetRunConfig('s') == "s1"):
            return "a,10;b,1"
        else:
            return "X,100;Y,200;"

class SmvRunConfigTest1(SmvBaseTest):

    @classmethod
    def smvAppInitArgs(cls):
        return ['--smv-props', 'smv.config.s=s1',
                '-m', "None"]

    def test_SmvCsvStringData_with_SmvRunConfig(self):
        fqn = self.__module__ + ".D4"
        df = self.df(fqn)
        expect = self.createDF("a:String;b:Integer", "a,10;b,1")
        self.should_be_same(expect, df)

class SmvRunConfigTest2(SmvBaseTest):

    @classmethod
    def smvAppInitArgs(cls):
        return ['--smv-props', 'smv.config.s=s2',
                '-m', "None"]

    def test_SmvCsvStringData_with_SmvRunConfig(self):
        fqn = self.__module__ + ".D4"
        df = self.df(fqn)
        expect = self.createDF("a:String;b:Integer", "X,100;Y,200")
        self.should_be_same(expect, df)
