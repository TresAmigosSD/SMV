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
from test_support.smvbasetest import SmvBaseTest
from smv import *
from smv.error import SmvDqmValidationError, SmvRuntimeError
from smv.modulesvisitor import ModulesVisitor
from smv.smvmodulerunner import SmvModuleRunner
from smv.smviostrategy import SmvJsonOnHdfsIoStrategy
from smv.smvmetadata import SmvMetaData, SmvMetaHistory

from pyspark.sql import DataFrame

cross_run_counter = 0
persist_run_counter = 0

class SmvFrameworkTest2(SmvBaseTest):
    @classmethod
    def smvAppInitArgs(cls):
        return ['--smv-props', 'smv.stages=stage']

    def test_visit_queue(self):
        fqns = ["stage.modules.M3", "stage.modules.M2"]
        ds = self.load2(*fqns)
        
        queue =  ModulesVisitor(ds).queue

        names = [m.fqn()[14:] for m in queue]
        self.assertEqual(names, ['I1', 'M1', 'M2', 'M3'])

    def test_basic_run(self):
        fqn = "stage.modules.M3"
        res = self.df2(fqn)

        exp = self.createDF(
            "a:Integer;b:Double",
            "1,0.3;0,0.2;3,0.5")
        self.should_be_same(res, exp)

    def test_basic_versioned_fqn(self):
        fqns = ["stage.modules.M3", "stage.modules.M2"]
        ds = self.load2(*fqns)

        self.assertNotEqual(ds[0].versioned_fqn, ds[1].versioned_fqn)

    def test_cross_tx_df_caching(self):
        """run method of a module should run only once even cross run tx"""
        # Reset cache and reset counter
        self.smvApp.df_cache = {}
        global cross_run_counter
        cross_run_counter = 0

        r1 = self.df2("stage.modules.M2")
        r2 = self.df2("stage.modules.M3")

        # counter is it M1 (ephemeral)
        self.assertEqual(cross_run_counter, 1)

    def test_persisted_df_should_run_only_once(self):
        """even reset df-cache, persisted module should only run once"""
        self.mkTmpTestDir()
        self.smvApp.df_cache = {}
        global persist_run_counter
        persist_run_counter = 0

        r1 = self.df2("stage.modules.M2")
        self.smvApp.df_cache = {}
        r2 = self.df2("stage.modules.M3")
        
        # counter is in M2 (non-ephemeral)
        self.assertEqual(persist_run_counter, 1)

    def test_basic_metadata_creation(self):
        fqn = "stage.modules.M2"
        m = self.load2(fqn)[0]

        SmvModuleRunner([m], self.smvApp).run()

        result = m.module_meta._metadata['_dqmValidation']
        rule_cnt = result['dqmStateSnapshot']['ruleErrors']['b_lt_04']['total']

        self.assertEqual(m.module_meta._metadata['_fqn'], fqn)
        self.assertEqual(rule_cnt, 1)

    def test_metadata_action_trigger_validation(self):
        """M3 is ephemeral and non-trivial metadata, it should trigger post_action"""

        fqn = "stage.modules.M3"
        m = self.load2(fqn)[0]

        runner = SmvModuleRunner([m], self.smvApp)

        # runner's run logic below, without the last force run
        mods_to_run_post_action = set(runner.visitor.queue)
        known = {}
        runner._create_df(known, mods_to_run_post_action)
        runner._create_meta(mods_to_run_post_action)

        # by now, since M3 has non-trivial metadata, it triggers post_action,
        # so should be removed from the mods_to_run_post_action

        self.assertEqual(len(mods_to_run_post_action), 0)

    def test_metadata_persist(self):
        self.mkTmpTestDir()
        fqn = "stage.modules.M1"
        m = self.load2(fqn)[0]
        meta_path = m.meta_path()

        self.df2(fqn)

        meta_json = SmvJsonOnHdfsIoStrategy(self.smvApp, meta_path).read()
        meta = SmvMetaData().fromJson(meta_json)
        self.assertEqual(meta._metadata['_fqn'], fqn)

        hist_dir = self.smvApp.all_data_dirs().historyDir
        hist_path = hist_path = "{}/{}.hist".format(hist_dir, fqn)
        hist_json = SmvJsonOnHdfsIoStrategy(self.smvApp, hist_path).read()
        hist = SmvMetaHistory().fromJson(hist_json)

        self.assertEqual(hist._hist_list[0]['_fqn'], fqn)

    def test_purge_persisted(self):
        fqn1 = "stage.modules.M2"
        fqn2 = "stage.modules.M3"

        (m1, m2) = self.load2(fqn1, fqn2)

        self.df2(fqn2)

        # Should be persisted
        self.assertTrue(os.path.exists(m1.persistStrategy()._csv_path))

        # Should still be there is purge_old
        SmvModuleRunner([m1], self.smvApp).purge_old_but_keep_new_persisted()
        self.assertTrue(os.path.exists(m1.persistStrategy()._csv_path))

        # Should be removed
        SmvModuleRunner([m2], self.smvApp).purge_persisted()
        self.assertFalse(os.path.exists(m1.persistStrategy()._csv_path))
        

    def test_publish(self):
        fqn = "stage.modules.M3"
        pub_dir = self.smvApp.all_data_dirs().publishDir

        m = self.load2(fqn)[0]
        SmvModuleRunner([m], self.smvApp).publish(pub_dir)

        csv_path = '{}/{}.csv'.format(pub_dir, m.fqn())
        meta_path = '{}/{}.meta'.format(pub_dir, m.fqn())
        hist_path = '{}/{}.hist'.format(pub_dir, m.fqn())

        self.assertTrue(os.path.exists(csv_path))
        self.assertTrue(os.path.exists(meta_path))
        self.assertTrue(os.path.exists(hist_path))




