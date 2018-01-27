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
    urn = 'mod:stage.modules.R4'

    @classmethod
    def smvAppInitArgs(cls):
        return ['--smv-props', 'smv.stages=stage']

    def setUp(self):
        """Tests in this class needs clean data directories"""
        shutil.rmtree(self.tmpDataDir(), ignore_errors=True)
        os.makedirs(self.tmpDataDir())

    def test_run_info_is_empty_if_no_module_is_run(self):
        # the first time modules are run, information is collected
        res, coll = self.smvApp.runModule(self.urn, forceRun=True)
        assert len(coll.fqns()) > 0

        # if we run again the collector should be empty because all
        # module results have been persisted so there would be no run
        # info collected
        res, coll = self.smvApp.runModule(self.urn, forceRun=False)
        assert len(coll.fqns()) == 0

    def test_get_run_info_is_empty_if_no_module_is_run(self):
        coll = self.smvApp.getRunInfo(self.urn)
        assert len(coll.fqns()) > 0  # still collected the dependencies

        for fqn in coll.fqns():
            assert len(coll.dqm_validation(fqn)) == 0
            assert len(coll.dqm_state(fqn)) == 0
            assert len(coll.metadata(fqn)) == 0
            assert len(coll.metadata_history(fqn)) == 0

    def test_get_run_info_should_return_info_from_last_run(self):
        self.smvApp.runModule(self.urn, forceRun=True)
        self.smvApp.runModule(self.urn, forceRun=False)
        coll = self.smvApp.getRunInfo(self.urn)
        for fqn in coll.fqns():
            if 'R2' not in fqn:  # R2 module does not have validation
                assert len(coll.dqm_validation(fqn)) > 0
                assert len(coll.dqm_state(fqn)) > 0
