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
import json

from test_support.smvbasetest import SmvBaseTest
from smv import *
from smv.dqm import *

import pyspark
from pyspark.context import SparkContext
from pyspark.sql import SQLContext, HiveContext
from pyspark.sql.functions import col, lit
from py4j.protocol import Py4JJavaError


class SmvFrameworkTest(SmvBaseTest):
    @classmethod
    def smvAppInitArgs(cls):
        return ['--smv-props', 'smv.stages=stage']

    def _escapeRegex(self, s):
        import re
        return re.sub(r"([\[\]\(\)])", r"\\\1", s)

    def test_SmvCsvStringData(self):
        fqn = "stage.modules.D1"
        df = self.df(fqn)
        expect = self.createDF("a:String;b:Integer", "x,10;y,1")
        self.should_be_same(expect, df)

    def test_SmvCsvStringData_with_error(self):
        fqn = "stage.modules.D1WithError"
        with self.assertRaisesRegexp(Py4JJavaError, "SmvDqmValidationError"):
            df = self.df(fqn)

    def test_SmvMultiCsvFiles(self):
        self.createTempInputFile("multiCsvTest/f1", "col1\na\n")
        self.createTempInputFile("multiCsvTest/f2", "col1\nb\n")
        self.createTempInputFile("multiCsvTest.schema", "col1: String\n")

        fqn = "stage.modules.MultiCsv"
        df = self.df(fqn)
        exp = self.createDF("col1: String", "a;b")
        self.should_be_same(df, exp)

    def test_SmvCsvFileWithUserSchema(self):
        self.createTempInputFile("test3.csv", "col1\na\nb\n")
        self.createTempInputFile("test3.schema", "col1: String\n")

        fqn = "stage.modules.CsvFile"
        df = self.df(fqn)
        exp = self.createDF("1loc: String", "a;b")
        self.should_be_same(df, exp)

    def test_SmvMultiCsvFilesWithUserSchema(self):
        self.createTempInputFile("test3/f1", "col1\na\n")
        self.createTempInputFile("test3/f2", "col1\nb\n")
        self.createTempInputFile("test3.schema", "col1: String\n")

        fqn = "stage.modules.MultiCsvWithUserSchema"
        df = self.df(fqn)
        exp = self.createDF("1loc: String", "a;b")
        self.should_be_same(df, exp)

    def test_SmvDQM(self):
        fqn = "stage.modules.D3"

        msg = """{"passed":false,"dqmStateSnapshot":{"totalRecords":3,"parseError":{"total":0,"firstN":[]},"fixCounts":{"a_lt_1_fix":1},"ruleErrors":{"b_lt_03":{"total":1,"firstN":["org.tresamigos.smv.dqm.DQMRuleError: b_lt_03 @FIELDS: b=0.5"]}}},"errorMessages":[{"FailTotalRuleCountPolicy(2)":"true"},{"FailTotalFixCountPolicy(1)":"false"},{"org.tresamigos.smv.SmvCsvStringData metadata validation":"true"},{"FailParserCountPolicy(1)":"true"},{"stage.modules.D3 metadata validation":"true"}],"checkLog":["Rule: b_lt_03, total count: 1","org.tresamigos.smv.dqm.DQMRuleError: b_lt_03 @FIELDS: b=0.5","Fix: a_lt_1_fix, total count: 1"]}"""

        with self.assertRaisesRegexp(Py4JJavaError, self._escapeRegex(msg)):
            df = self.df(fqn)
            df.smvDumpDF()

    def test_SmvSqlModule(self):
        fqn = "stage.modules.SqlMod"
        exp = self.createDF("a: String; b: String", "def,mno;ghi,jkl")
        df = self.df(fqn)
        self.should_be_same(df, exp)

        # verify that the tables have been dropped
        tablesDF = self.smvApp.sqlContext.tables()
        tableNames = [r.tableName for r in tablesDF.collect()]
        self.assertNotIn("a", tableNames)
        self.assertNotIn("b", tableNames)

    def test_SmvSqlCsvFile(self):
        self.createTempInputFile("test3.csv", "a,b,c\na1,100,c1\na2,200,c2\n")
        self.createTempInputFile("test3.schema", "a: String;b: Integer;c: String\n")

        fqn = "stage.modules.SqlCsvFile"
        df = self.df(fqn)
        exp = self.createDF("a: String; b:Integer",
             """a1,100;
                a2,200""")
        self.should_be_same(df, exp)

        # verify that the table have been dropped
        tablesDF = self.smvApp.sqlContext.tables()
        tableNames = [r.tableName for r in tablesDF.collect()]
        self.assertNotIn("a", tableNames)

    #TODO: add other SmvDataSet unittests

class SmvRunConfigTest1(SmvBaseTest):

    @classmethod
    def smvAppInitArgs(cls):
        return ['--smv-props', 'smv.config.s=s1', 'smv.config.i=1', 'smv.config.b=True', 'smv.stages=stage',
                '-m', "None"]

    def test_SmvCsvStringData_with_SmvRunConfig(self):
        fqn = "stage.modules.D4"
        df = self.df(fqn)
        expect = self.createDF("a: String;b: Integer",
            """test1_s1,1;
                test2_not_i2,4;
                test3_b,5""")
        self.should_be_same(expect, df)

class SmvRunConfigTest2(SmvBaseTest):

    @classmethod
    def smvAppInitArgs(cls):
        return ['--smv-props', 'smv.config.s=s2', 'smv.config.i=2', 'smv.config.b=false', 'smv.stages=stage',
                '-m', "None"]

    def test_SmvCsvStringData_with_SmvRunConfig(self):
        fqn = "stage.modules.D4"
        df = self.df(fqn)
        expect = self.createDF("a:String;b:Integer",
            """test1_not_s1,2;
                test2_i2,3;
                test3_not_b,6""")
        self.should_be_same(expect, df)

class SmvNameErrorPropagationTest(SmvBaseTest):
    @classmethod
    def smvAppInitArgs(cls):
        return ['--smv-props', 'smv.stages=stage', '-m', "None"]

    def test_module_NameError_propagation(self):
        fqn = "stage.modules.ModWithBadName"
        with self.assertRaisesRegexp(Py4JJavaError, "NameError"):
            self.df(fqn)


class SmvSyntaxErrorPropagationTest(SmvBaseTest):
    @classmethod
    def smvAppInitArgs(cls):
        return ['--smv-props', 'smv.stages=syntax_error_stage', '-m', "None"]

    def test_module_SyntaxError_propagation(self):
        fqn = "syntax_error_stage.modules.ModWithBadSyntax"
        with self.assertRaisesRegexp(Py4JJavaError, "SyntaxError"):
            self.df(fqn)

class SmvMetadataTest(SmvBaseTest):
    @classmethod
    def smvAppInitArgs(cls):
        return ['--smv-props', 'smv.stages=metadata_stage', '-m', "None"]

    def test_metadata_includes_user_metadata(self):
        fqn = "metadata_stage.modules.ModWithUserMeta"
        self.df(fqn)
        with open(self.tmpDataDir() + "/history/{}.hist/part-00000".format(fqn)) as f:
            metadata_list = json.loads(f.read())
            metadata = metadata_list['history'][0]
        self.assertEqual(metadata['foo'], "bar")

    def test_metadata_validation_failure_causes_error(self):
        fqn = "metadata_stage.modules.ModWithFailingValidation"
        with self.assertRaisesRegexp(Py4JJavaError, "SmvDqmValidationError"):
            self.df(fqn)

    def test_invalid_metadata_rejected_gracefully(self):
        fqn = "metadata_stage.modules.ModWithInvalidMetadata"
        with self.assertRaisesRegexp(Exception, r"metadata .* is not a dict"):
            self.df(fqn)

    def test_invalid_metadata_validation_rejected_gracefully(self):
        fqn = "metadata_stage.modules.ModWithInvalidMetadataValidation"
        with self.assertRaisesRegexp(Exception, r"message .* is not a string"):
            self.df(fqn)
