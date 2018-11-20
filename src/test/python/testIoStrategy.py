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

class SmvIoStrategyTest(SmvBaseTest):
    @classmethod
    def smvAppInitArgs(cls):
        return ['--smv-props', 'smv.stages=stage']

    def test_parquet_strategy(self):
        self.smvApp.setDynamicRunConfig({})
        fqn = "stage.modules.M2"
        res = self.df(fqn)
        exp = self.createDF("k:String;v:Integer", "a,1;b,2")
        self.should_be_same(res, exp)

        # M1 is parquet
        mod = self.load("stage.modules.M1")[0]
        smphr = mod.persistStrategy()._semaphore_path
        self.assertTrue(os.path.exists(smphr))

    def test_default_strategy_to_parquet(self):
        self.smvApp.setDynamicRunConfig({
            'smv.sparkdf.defaultPersistFormat': 'parquet_on_hdfs'
        })
        fqn = "stage.modules.M2"
        res = self.df(fqn)
        mod = self.load(fqn)[0]
        self.assertTrue(mod.persistStrategy()._file_path.endswith(".parquet"))
        self.assertTrue(os.path.exists(mod.persistStrategy()._file_path))

    def test_default_strategy_to_csv(self):
        self.smvApp.setDynamicRunConfig({
            'smv.sparkdf.defaultPersistFormat': 'smvcsv_on_hdfs'
        })
        fqn = "stage.modules.M2"
        res = self.df(fqn)
        mod = self.load(fqn)[0]
        self.assertTrue(mod.persistStrategy()._file_path.endswith(".csv"))
        self.assertTrue(os.path.exists(mod.persistStrategy()._file_path))
