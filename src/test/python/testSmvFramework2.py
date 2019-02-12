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

import os
import sys
from test_support.smvbasetest import SmvBaseTest
from smv import *
from smv.error import SmvDqmValidationError, SmvRuntimeError
from smv.modulesvisitor import ModulesVisitor
from smv.smvmodulerunner import SmvModuleRunner
from smv.smviostrategy import SmvJsonOnHdfsPersistenceStrategy
from smv.smvmetadata import SmvMetaData, SmvMetaHistory

from pyspark.sql import DataFrame

cross_run_counter = 0
persist_run_counter = 0
m1_post_counter = 0

class SmvFrameworkTest2(SmvBaseTest):
    @classmethod
    def smvAppInitArgs(cls):
        return ['--smv-props', 'smv.stages=stage']

    def setUp(self):
        super(SmvFrameworkTest2, self).setUp()
        # Clean up data_cache, persisted files, and dynamic conf for each test
        self.smvApp.data_cache = {}
        self.mkTmpTestDir()
        self.smvApp.setDynamicRunConfig({})

    def test_visit_queue(self):
        fqns = ["stage.modules.M3", "stage.modules.M2"]
        ds = self.load(*fqns)

        queue =  ModulesVisitor(ds).queue

        names = [m.fqn()[14:] for m in queue]
        self.assertEqual(names, ['I1', 'M1', 'M2', 'M3'])

    def test_basic_run(self):
        fqn = "stage.modules.M3"
        res = self.df(fqn)

        exp = self.createDF(
            "a:Integer;b:Double",
            "1,0.3;0,0.2;3,0.5")
        self.should_be_same(res, exp)

    def test_basic_versioned_fqn(self):
        fqns = ["stage.modules.M3", "stage.modules.M2"]
        ds = self.load(*fqns)

        self.assertNotEqual(ds[0].versioned_fqn, ds[1].versioned_fqn)

    def test_cross_tx_df_caching(self):
        """run method of a module should run only once even cross run tx"""
        # Reset counter
        global cross_run_counter
        cross_run_counter = 0

        r1 = self.df("stage.modules.M2")
        r2 = self.df("stage.modules.M3")

        # counter is it M1 (ephemeral)
        self.assertEqual(cross_run_counter, 1)

    def test_persisted_df_should_run_only_once(self):
        """even reset df-cache, persisted module should only run once"""
        global persist_run_counter
        persist_run_counter = 0

        r1 = self.df("stage.modules.M2")
        self.smvApp.data_cache = {}
        r2 = self.df("stage.modules.M3")

        # counter is in M2 (non-ephemeral)
        self.assertEqual(persist_run_counter, 1)

    def test_basic_metadata_creation(self):
        fqn = "stage.modules.M2"
        m = self.load(fqn)[0]

        SmvModuleRunner([m], self.smvApp).run()

        result = m.module_meta._metadata['_dqmValidation']
        rule_cnt = result['dqmStateSnapshot']['ruleErrors']['b_lt_04']['total']

        self.assertEqual(m.module_meta._metadata['_fqn'], fqn)
        self.assertEqual(rule_cnt, 1)

    def test_metadata_persist(self):
        fqn = "stage.modules.M1"
        m = self.load(fqn)[0]
        meta_path = m._meta_path()

        self.df(fqn)

        meta_json = SmvJsonOnHdfsPersistenceStrategy(self.smvApp, meta_path).read()
        meta = SmvMetaData().fromJson(meta_json)
        self.assertEqual(meta._metadata['_fqn'], fqn)

        hist_dir = self.smvApp.all_data_dirs().historyDir
        hist_path = hist_path = "{}/{}.hist".format(hist_dir, fqn)
        hist_json = SmvJsonOnHdfsPersistenceStrategy(self.smvApp, hist_path).read()
        hist = SmvMetaHistory().fromJson(hist_json)

        self.assertEqual(hist._hist_list[0]['_fqn'], fqn)

    def test_purge_persisted(self):
        fqn1 = "stage.modules.M2"
        fqn2 = "stage.modules.M3"

        (m1, m2) = self.load(fqn1, fqn2)

        self.df(fqn2)

        # Should be persisted
        self.assertTrue(os.path.exists(m1.persistStrategy()._file_path))

        # Should be removed
        SmvModuleRunner([m2], self.smvApp).purge_persisted()
        self.assertFalse(os.path.exists(m1.persistStrategy()._file_path))


    def test_publish(self):
        fqn = "stage.modules.M3"
        pub_dir = self.smvApp.all_data_dirs().publishDir

        m = self.load(fqn)[0]
        SmvModuleRunner([m], self.smvApp).publish(pub_dir)

        csv_path = '{}/{}.csv'.format(pub_dir, m.fqn())
        meta_path = '{}/{}.meta'.format(pub_dir, m.fqn())
        hist_path = '{}/{}.hist'.format(pub_dir, m.fqn())

        self.assertTrue(os.path.exists(csv_path))
        self.assertTrue(os.path.exists(meta_path))
        self.assertTrue(os.path.exists(hist_path))


    def test_quick_run(self):
        fqn1 = "stage.modules.M1"
        fqn3 = "stage.modules.M3"
        df1 = self.df(fqn1)
        df3 = self.smvApp.quickRunModule(fqn3)[0]
        self.should_be_same(df1, df3)

    def test_need_to_run_list(self):
        self.df("stage.modules.M2")

        ds = self.load("stage.modules.M5")[0]
        res = ModulesVisitor([ds]).modules_needed_for_run
        names = [m.fqn()[14:] for m in res]
        self.assertEqual(names, ['M2', 'M5'])

    def test_ephemeral_dqm_will_not_run_if_not_needed(self):
        global m1_post_counter
        m1_post_counter = 0

        # M1's post_action should run once here
        self.df("stage.modules.M2")
        self.assertEqual(m1_post_counter, 1)

        # M1's post_action should not run again
        self.df("stage.modules.M5")
        self.assertEqual(m1_post_counter, 1)

    def test_try_to_load_non_exit_module(self):
        with self.assertRaisesRegexp(SmvRuntimeError, "Module .*NonExist does not exist"):
            self.load("stage.modules.NonExist")

    def test_app_get_need_to_run(self):
        self.df("stage.modules.I1")

        ms = self.load("stage.modules.M5")
        names = [m.fqn()[14:] for m in self.smvApp.get_need_to_run(ms)]
        self.assertEqual(names, ['M2', 'M5'])

    def test_lib_with_clib(self):
        self.df("stage.modules.WithLib")

    def test_error_on_wrong_type(self):
        with self.assertRaisesRegexp(SmvRuntimeError, "should be a Spark DataFrame"):
            self.df("stage.modules.WrongType")

class SmvForceEddTest(SmvBaseTest):
    @classmethod
    def smvAppInitArgs(cls):
        return ['--smv-props', 'smv.stages=stage']

    def setUp(self):
        super(SmvForceEddTest, self).setUp()
        # Clean up data_cache, persisted files, and dynamic conf for each test
        self.smvApp.data_cache = {}
        self.mkTmpTestDir()
        self.smvApp.setDynamicRunConfig({})

    def test_no_force_create_edd(self):
        fqn = "stage.modules.M2"
        (df, info) = self.smvApp.runModule(fqn)
        meta = info.metadata(fqn)
        edd = meta.get('_edd')
        self.assertEqual(len(edd), 0)

    def test_force_create_edd(self):
        fqn = "stage.modules.M2"
        self.smvApp.setDynamicRunConfig({'smv.forceEdd': 'True'})
        (df, info) = self.smvApp.runModule(fqn)
        meta = info.metadata(fqn)
        edd = meta.get('_edd')
        for r in edd:
            if (r['taskName'] == 'cnt' and r['colName'] == 'a'):
                self.assertEqual(r['valueJSON'], '2')

class SmvAppWithoutSparkTest(SmvBaseTest):
    @classmethod
    def setUpClass(cls):
        from smv.smvapp import SmvApp
        from test_support.testconfig import TestConfig

        args = TestConfig.smv_args() + cls.smvAppInitArgs() + ['--data-dir', cls.tmpDataDir()]
        # The test's SmvApp must be set as the singleton for correct results of some tests
        # The original SmvApp (if any) will be restored when the test is torn down
        cls.smvApp = SmvApp.createInstance(args, None)

        sys.path.append(cls.resourceTestDir())

        cls.mkTmpTestDir()

    @classmethod
    def smvAppInitArgs(cls):
        return ['--smv-props', 'smv.stages=stage']

    def test_load_without_sc(self):
        fqn = "stage.modules.M2"
        m = self.load(fqn)[0]
        self.assertEqual(m.resolvedRequiresDS[0].fqn(), "stage.modules.M1")

    def test_get_graph_without_sc(self):
        self.smvApp.get_graph_json()
