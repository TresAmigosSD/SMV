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
import shutil

from test_support.smvbasetest import SmvBaseTest


class SmvRunInfoTest(SmvBaseTest):
    R4Fqn = 'stage1.modules.R4'
    R5Fqn = 'stage2.modules.R5'

    @classmethod
    def smvAppInitArgs(cls):
        return ['--smv-props', 'smv.stages=stage1:stage2']

    def setUp(self):
        """Tests in this class needs clean data directories"""
        shutil.rmtree(self.tmpDataDir(), ignore_errors=True)
        os.makedirs(self.tmpDataDir())

    def test_run_info_is_empty_if_no_module_is_run(self):
        # the first time modules are run, information is collected
        res, coll = self.smvApp.runModule(self.R4Fqn, forceRun=True)
        assert len(coll.fqns()) > 0

        # if we run again the collector should be empty because all
        # module results have been persisted so there would be no run
        # info collected
        res, coll = self.smvApp.runModule(self.R4Fqn, forceRun=False)
        assert len(coll.fqns()) == 0

    def test_get_run_info_is_empty_if_no_module_is_run(self):
        coll = self.smvApp.getRunInfo(self.R4Fqn)
        assert len(coll.fqns()) > 0  # still collected the dependencies

        for fqn in coll.fqns():
            assert len(coll.dqm_validation(fqn)) == 0
            assert len(coll.dqm_state(fqn)) == 0
            assert len(coll.metadata(fqn)['_fqn']) > 0  # still collect basic meta

    def test_get_run_info_should_return_info_from_last_run(self):
        self.smvApp.runModule(self.R4Fqn, forceRun=True)
        self.smvApp.runModule(self.R4Fqn, forceRun=False)
        coll = self.smvApp.getRunInfoByPartialName('R4')
        for fqn in coll.fqns():
            if 'R2' not in fqn:  # R2 module does not have validation
                assert len(coll.dqm_validation(fqn)) > 0
                assert len(coll.dqm_state(fqn)) > 0

    # Target a bug that caused an error when getting run info for a module
    # that depends on a link
    def test_get_run_info_of_module_with_link_dependency(self):
        self.smvApp.runModule(self.R5Fqn, forceRun=True)
        # This will fail if there is a problem due to link dependency
        info = self.smvApp.getRunInfo(self.R5Fqn)
