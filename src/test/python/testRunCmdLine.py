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


class RunModuleFromCmdLineTest(SmvBaseTest):
    modUrn = 'runmod.stage1.modules.A'

    @classmethod
    def smvAppInitArgs(cls):
        return ['--smv-props', 'smv.stages=runmod.stage1',
                '-m', "modules.A"]

    def test_can_run_module_from_cmdline(self):
        self.smvApp.j_smvApp.run()
        a = self.df(self.modUrn)
        expected = self.createDF("k:String;v:Integer", "a,;b,2")
        self.should_be_same(a, expected)


class RunStageFromCmdLineTest(SmvBaseTest):
    stageName = 'runstage.stage1'

    @classmethod
    def smvAppInitArgs(cls):
        return ['--smv-props', 'smv.stages=runstage.stage1',
                '-s', cls.stageName]

    def test_can_run_stage_from_cmdline(self):
        self.smvApp.j_smvApp.run()
        a = self.df(self.stageName + ".modules.A")
        self.should_be_same(a, self.createDF("k:String;v:Integer", "a,;b,2"))
        b = self.df(self.stageName + ".modules.B")
        self.should_be_same(b, self.createDF("k:String;v:Integer", "c,3;d,4"))
