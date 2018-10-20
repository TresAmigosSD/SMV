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

from test_support.smvbasetest import SmvBaseTest
from smv import *
from smv.error import SmvDqmValidationError, SmvRuntimeError
from smv.smvdataset import ModulesVisitor
from smv.smvmodulerunner import SmvModuleRunner

from pyspark.sql import DataFrame

cross_run_counter = 0

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
        ds = self.load2(fqn)
        res = SmvModuleRunner(ds).run()[0]

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

        fqns = ["stage.modules.M2", "stage.modules.M3"]
        ds = self.load2(*fqns)

        r1 = SmvModuleRunner([ds[0]]).run()[0]
        r2 = SmvModuleRunner([ds[1]]).run()[0]

        self.assertEqual(cross_run_counter, 1)