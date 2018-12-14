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
from smv.error import SmvRuntimeError

class RunModuleWithRunConfigTest(SmvBaseTest):
    modFqn = 'stage.modules.A'
    modName = 'modules.A'

    @classmethod
    def smvAppInitArgs(cls):
        return ['--smv-props', 'smv.stages=stage',
                'smv.config.keys=src', 'smv.config.src=cmd',
                '-m', "modules.A"]

    def test_run_module_with_cmd_run_config(self):
        self.smvApp.run()
        self.smvApp.setDynamicRunConfig({})
        res = self.df(self.modFqn)
        expected = self.createDF('src:String', 'cmd')
        self.should_be_same(expected, res)

    def test_run_module_with_dynamic_run_config(self):
        self.smvApp.run()
        self.smvApp.setDynamicRunConfig({})
        self.smvApp.setDynamicRunConfig({'src': 'dynamic_a'})
        a = self.df(self.modFqn)
        self.should_be_same(self.createDF('src:String', 'dynamic_a'), a)
        self.smvApp.setDynamicRunConfig({'src': 'dynamic_b'})
        b = self.df(self.modFqn)
        self.should_be_same(self.createDF('src:String', 'dynamic_b'), b)

        # Default runConfig=None leads to no change on the dynamic config
        c = self.df(self.modFqn)
        self.should_be_same(self.createDF('src:String', 'dynamic_b'), c)

    def test_run_module_by_name_with_run_config(self):
        self.smvApp.setDynamicRunConfig({})
        df, collector = self.smvApp.runModuleByName(self.modName)
        expected = self.createDF('src:String', 'cmd')
        self.should_be_same(expected, df)

    def test_dynamic_data_dir_change(self):
        init_output_python = self.smvApp.outputDir()
        self.smvApp.setDynamicRunConfig({'smv.outputDir': 'dummy'})
        output_python = self.smvApp.outputDir()
        self.assertEqual(output_python, 'dummy')

    def test_explicit_set_dynamic_run_config(self):
        self.smvApp.setDynamicRunConfig({'src': 'dynamic_a'})
        a = self.df(self.modFqn)
        self.should_be_same(self.createDF('src:String', 'dynamic_a'), a)

    def test_use_without_requiresConfig_should_error_out(self):
        with self.assertRaisesRegexp(SmvRuntimeError, "RunConfig key .* was not specified"):
            self.df("stage.modules.RunConfWithError")
