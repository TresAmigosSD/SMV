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
from test_support.extrapath import ExtraPath

class SmvModelTest(SmvBaseTest):
    @classmethod
    def smvAppInitArgs(cls):
        return ["--smv-props", "smv.stages=stage1:stage2"]

    def test_SmvModelExec(self):
        with ExtraPath("src/test/python/smv_model"):
            model = self.smvApp.getModuleResult("stage1.modules.Model")
            execDf = self.df("stage1.modules.ModelExec")

        self.assertEqual(str(model), execDf.collect()[0][0])

    def test_link_to_SmvResultModule(self):
        """Test that result of link to SmvModel is same as SmvModel's result
        """
        with ExtraPath("src/test/python/smv_model"):
            ModelRes = self.smvApp.getModuleResult("stage1.modules.Model")
            ModelExecDf = self.smvApp.getModuleResult("stage2.modules.ModelExecWithLink")
        self.assertEqual(str(ModelRes), ModelExecDf.collect()[0][0])

    def test_module_depends_on_model(self):
        """Test module can depends on model and use directly"""
        with ExtraPath("src/test/python/smv_model"):
            mod = self.df("stage1.modules.Model")
            res = self.df("stage1.modules.ModuleUsesModel")
        exp = self.createDF("a:String", "\"{}\"".format(mod))
        self.should_be_same(res, exp)
