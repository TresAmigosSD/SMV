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
from smv import SmvApp

class RunModuleWithoutRunConfigTest(SmvBaseTest):
    modUrn = 'mod:stage.modules.A'

    @classmethod
    def smvAppInitArgs(cls):
        return ['--smv-props', 'smv.stages=stage',
                '-m', "modules.A"]

    def test_run_module_without_run_config(self):
        self.smvApp.j_smvApp.run()
        res = self.smvApp.runModule(self.modUrn)
        expected = self.createDF('src:String', '')
        self.should_be_same(res, expected)

class RunModuleWithRunConfigTest(SmvBaseTest):
    modUrn = 'mod:stage.modules.A'

    @classmethod
    def smvAppInitArgs(cls):
        return ['--smv-props', 'smv.stages=stage',
                'smv.config.keys=src', 'smv.config.src=cmd',
                '-m', "modules.A"]

    def test_run_module_with_cmd_run_config(self):
        self.smvApp.j_smvApp.run()
        res = self.smvApp.runModule(self.modUrn)
        expected = self.createDF('src:String', 'cmd')
        self.should_be_same(res, expected)

    def test_run_module_with_dynamic_run_config(self):
        self.smvApp.j_smvApp.run()
        a = self.smvApp.runModule(self.modUrn, runConfig = {'src': 'dynamic_a'})
        self.should_be_same(a, self.createDF('src:String', 'dynamic_a'))
        b = self.smvApp.runModule(self.modUrn, runConfig = {'src': 'dynamic_b'})
        self.should_be_same(b, self.createDF('src:String', 'dynamic_b'))
