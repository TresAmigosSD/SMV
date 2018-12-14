# -*- coding: utf-8 -*-
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
from test_support.testconfig import TestConfig
from smv import *
import sys
import smv.utils
import smv.smvshell
from smv.dqm import *
from smv.error import SmvDqmValidationError, SmvMetadataValidationError, SmvRuntimeError

import pyspark
from pyspark.context import SparkContext
from pyspark.sql import SQLContext, HiveContext, DataFrame
from pyspark.sql.functions import col, lit


single_run_counter = 0
metadata_count = 0
module_load_count = 0

class SmvFrameworkTest(SmvBaseTest):
    @classmethod
    def smvAppInitArgs(cls):
        return ['--smv-props', 'smv.stages=stage:stage2:cycle']

    def test_SmvDQM(self):
        fqn = "stage.modules.D3"

        msg = """{"passed":false,"dqmStateSnapshot":{"totalRecords":3,"parseError":{"total":0,"firstN":[]},"fixCounts":{"a_lt_1_fix":1},"ruleErrors":{"b_lt_03":{"total":1,"firstN":["org.tresamigos.smv.dqm.DQMRuleError: b_lt_03 @FIELDS: b=0.5"]}}},"errorMessages":[{"FailTotalRuleCountPolicy(2)":"true"},{"FailTotalFixCountPolicy(1)":"false"},{"FailParserCountPolicy(1)":"true"}],"checkLog":["Rule: b_lt_03, total count: 1","org.tresamigos.smv.dqm.DQMRuleError: b_lt_03 @FIELDS: b=0.5","Fix: a_lt_1_fix, total count: 1"]}"""

        with self.assertRaises(SmvDqmValidationError) as cm:
            df = self.df(fqn)
            df.smvDumpDF()

        e = cm.exception
        self.assertEqual(e.dqmValidationResult["passed"], False)
        self.assertEqual(e.dqmValidationResult["dqmStateSnapshot"]["totalRecords"], 3)
        self.assertEqual(e.dqmValidationResult["dqmStateSnapshot"]["parseError"]["total"],0)
        self.assertEqual(e.dqmValidationResult["dqmStateSnapshot"]["fixCounts"]["a_lt_1_fix"],1)
        self.assertEqual(e.dqmValidationResult["dqmStateSnapshot"]["ruleErrors"]["b_lt_03"]["total"],1)

    # All SmvInput related tests were moved to testSmvInput.py

    def test_depends_on_other_stage_output_module_wo_link_should_pass(self):
        self.createTempInputFile("test3.csv", "col1\na\nb\n")
        self.createTempInputFile("test3.schema", "col1: String\n")
        fqn = "stage2.modules.DependsOutputModuleDirectly"
        df = self.df(fqn)

    def test_depends_on_other_stage_non_output_module_wo_link_should_pass(self):
        fqn = "stage2.modules.DependsNonOutputModuleDirectly"
        df = self.df(fqn)

    def test_depends_through_link_should_pass(self):
        self.createTempInputFile("test3.csv", "col1\na\nb\n")
        self.createTempInputFile("test3.schema", "col1: String\n")
        fqn = "stage2.modules.DependsOnLink"
        df = self.df(fqn)

    def test_module_persist_with_null(self):
        # Module has both empty string and null value in string, should read back as the same
        fqn = "stage.modules.CsvStrWithNullData"
        df = self.df(fqn, True)
        m = self.load(fqn)[0]
        read_back = m.persistStrategy().read()
        self.should_be_same(df, read_back)

    def test_cycle_dependency_error_out(self):
        fqn = "cycle.modules.CycleA"
        with self.assertRaisesRegexp(SmvRuntimeError, "Cycle found while resolving"):
            df = self.df(fqn)

    def test_module_should_only_run_once(self):
        fqnA = "cycle.modules.SingleRunA"
        fqnB = "cycle.modules.SingleRunB"
        fqnC = "cycle.modules.SingleRunC"

        b_res = self.df(fqnB)
        b_res.count()
        c_res = self.df(fqnC)
        c_res.count()

        self.assertEqual(single_run_counter, 1)


class SmvRunConfigTest(SmvBaseTest):

    @classmethod
    def smvAppInitArgs(cls):
        return ['--smv-props', 'smv.config.s=s2', 'smv.config.i=2', 'smv.config.b=false', 'smv.config.c=c',
                'smv.config.one=1', 'smv.config.bool=True', 'smv.stages=stage', '-m', "None"]

    def test_SmvModule_with_SmvRunConfig(self):
        fqn = "stage.modules.D4"
        df = self.df(fqn)
        expect = self.createDF("a:String;b:Integer",
            """test1_not_s1,2;
                test2_i2,3;
                test3_not_b,6;
                test4_defined_c,8;
                test5_defined_one,10;
                test6_defined_bool,12""")
        self.should_be_same(expect, df)

class SmvNameErrorPropagationTest(SmvBaseTest):
    @classmethod
    def smvAppInitArgs(cls):
        return ['--smv-props', 'smv.stages=stage', '-m', "None"]

    def test_module_NameError_propagation(self):
        fqn = "stage.modules.ModWithBadName"
        with self.assertRaisesRegexp(NameError, "ModWhoseNameDoesntExist"):
            self.df(fqn)


class SmvSyntaxErrorPropagationTest(SmvBaseTest):
    @classmethod
    def smvAppInitArgs(cls):
        return ['--smv-props', 'smv.stages=syntax_error_stage', '-m', "None"]

    def test_module_SyntaxError_propagation(self):
        fqn = "syntax_error_stage.modules.ModWithBadSyntax"
        with self.assertRaises(SyntaxError):
            self.df(fqn)

# TODO: this should be moved into own file.
class SmvMetadataTest(SmvBaseTest):
    @classmethod
    def smvAppInitArgs(cls):
        return ['--smv-props', 'smv.stages=metadata_stage', '-m', "None"]

    def test_metadata_includes_user_metadata(self):
        fqn = "metadata_stage.modules.ModWithUserMeta"
        self.df(fqn)
        with open(self.tmpDataDir() + "/history/{}.hist".format(fqn)) as f:
            metadata_list = json.loads(f.read())
            metadata = metadata_list['history'][0]
        self.assertEqual(metadata['_userMetadata']['foo'], "bar")

    def test_metadata_validation_failure_causes_error(self):
        fqn = "metadata_stage.modules.ModWithFailingValidation"
        with self.assertRaises(SmvMetadataValidationError):
            self.df(fqn)

    def test_invalid_metadata_rejected_gracefully(self):
        fqn = "metadata_stage.modules.ModWithInvalidMetadata"
        with self.assertRaisesRegexp(Exception, r"metadata .* is not a dict"):
            self.df(fqn)

    def test_invalid_metadata_validation_rejected_gracefully(self):
        fqn = "metadata_stage.modules.ModWithInvalidMetadataValidation"
        with self.assertRaisesRegexp(Exception, r"message .* is not a string"):
            self.df(fqn)

    def test_metadata_only_called_once(self):
        # running the module will incr the global metadata count by 1 for each
        # call to metadata
        #
        # Even if the module isEphemeral user meta should only run once
        fqn1 = "metadata_stage.modules.ModWithMetaCount"
        fqn2 = "metadata_stage.modules.DependsOnMetaCount"
        self.df(fqn1).count()
        self.df(fqn2).count()

        # Note: must import AFTER `df` above to get latest instance of package!
        self.assertEqual(metadata_count, 1)

class SmvNeedsToRunTest(SmvBaseTest):
    @classmethod
    def smvAppInitArgs(cls):
        return ['--smv-props', 'smv.stages=stage']

    @classmethod
    def deleteModuleOutput(cls, m):
        m.persistStrategy().remove()
        m.metaStrategy().remove()

    def test_input_module_does_not_need_to_run(self):
        fqn = "stage.modules.CsvFile"
        m = self.load(fqn)[0]
        self.assertFalse(m.needsToRun())

    def test_module_not_persisted_should_need_to_run(self):
        fqn = "stage.modules.NeedRunM1"
        m = self.load(fqn)[0]
        self.deleteModuleOutput(m)
        self.assertTrue(m.needsToRun())

    def test_module_persisted_should_not_need_to_run(self):
        fqn = "stage.modules.NeedRunM1"
        # Need to force run, since the df might in cache while the persisted file get deleted
        self.df(fqn, forceRun=True)
        self.assertFalse(self.load(fqn)[0].needsToRun())

    def test_ephemeral_module_depends_on_not_need_to_run_also_not(self):
        fqn = "stage.modules.NeedRunM3"
        fqn0 = "stage.modules.NeedRunM1"
        self.df(fqn0, True)
        self.assertFalse(self.load(fqn)[0].needsToRun())

    def test_ephemeral_module_depends_on_need_to_run_also_need(self):
        fqn = "stage.modules.NeedRunM3"
        fqn0 = "stage.modules.NeedRunM1"
        self.df(fqn, True)
        self.deleteModuleOutput(self.load(fqn0)[0]) # deleting persist files made M1 need to run
        self.assertTrue(self.load(fqn)[0].needsToRun())

class SmvPublishTest(SmvBaseTest):
     @classmethod
     def smvAppInitArgs(cls):
         return [
             '--smv-props',
             'smv.stages=stage',
             '-m',
             'stage.modules.CsvFile',
             '--publish',
             'v1'
         ]

     def test_publish_as_file(self):
         self.createTempInputFile("test3.csv", "col1\na\nb\n")
         self.createTempInputFile("test3.schema", "col1: String\n")
         fqn = "stage.modules.CsvFile"
         self.smvApp.run()

         # Read from the file
         res = smv.smvshell.openCsv(self.tmpDataDir() + "/publish/v1/" + fqn + ".csv")
         expected = self.createDF("col1: String", "a;b")

         self.should_be_same(res, expected)

class SmvAppPyHotLoadTest(SmvBaseTest):
    @classmethod
    def smvAppInitArgs(cls):
        return ['--smv-props', 'smv.stages=hotload', '-m', "None"]

    def test_with_hotload(self):
        """Module M1 should be loaded twice"""
        global module_load_count
        module_load_count = 0
        fqn = "hotload.modules.M1"
        self.load(fqn)
        self.load(fqn)
        # module_load_count is defined in this file. It is imported by python module hotload.modules.
        # When the fqn is loaded, the module hotload.modules is loaded. When we don't do hotload on
        # python modules, python itself will only load each module once, but when we have hotload
        # switched on, we clean up sys.modules each time when call self.load(fqn), so here the counter
        # get accumulated twice as the fqn is loaded twice
        self.assertEqual(module_load_count, 2)

class SmvAppNoPyHotLoadTest(SmvBaseTest):
    @classmethod
    def smvAppInitArgs(cls):
        return ['--smv-props', 'smv.stages=hotload', '-m', "None"]

    @classmethod
    def setUpClass(cls):
        from smv.smvapp import SmvApp

        cls.sparkSession = TestConfig.sparkSession()
        cls.sparkContext = TestConfig.sparkContext()
        cls.sparkContext.setLogLevel("ERROR")

        args = TestConfig.smv_args() + cls.smvAppInitArgs() + ['--data-dir', cls.tmpDataDir()]
        # set py_module_hotload flag to False so no reload of python files
        cls.smvApp = SmvApp.createInstance(args, cls.sparkSession, py_module_hotload=False)

        sys.path.append(cls.resourceTestDir())

        cls.mkTmpTestDir()

    def test_without_hotload(self):
        """Module M1 should only be loaded once"""
        global module_load_count
        module_load_count = 0
        fqn = "hotload.modules.M1"
        self.load(fqn)
        self.load(fqn)
        # As we switched off module hotload, the hotload.modules python module
        # will be only load once. If it was loaded once before this test run
        # module_load_count will be 0, otherwise, will be 1. It will not be
        # 2 as in test_with_hotload
        self.assertTrue(module_load_count <= 1)


class SmvUnicodeTest(SmvBaseTest):
    @classmethod
    def smvAppInitArgs(cls):
         return [
             '--smv-props',
             'smv.stages=unicode'
        ]

    def test_unicode_in_code(self):
        res = self.df("unicode.modules.ModWithUnicode")
        exp = self.createDF("a:String", "哈哈")
        self.should_be_same(res, exp)


