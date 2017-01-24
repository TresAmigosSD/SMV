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

from smvbasetest import SmvBaseTest
from smv import smvPy

class RunModuleFromCmdLineTest(SmvBaseTest):
    modUrn = 'fixture.cmdline.runmod.stage1.modules.A'

    @classmethod
    def smvAppInitArgs(cls):
        return ['--smv-props', 'smv.stages=fixture.cmdline.runmod.stage1',
                '-m', "modules.A"]

    def test_can_run_module_from_cmdline(self):
        smvPy.j_smvApp.run()
        a = smvPy.runModule(self.modUrn)
        expected = self.createDF("k:String;v:Integer", "a,;b,2")
        self.should_be_same(a, expected)
