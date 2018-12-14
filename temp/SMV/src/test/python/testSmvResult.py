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

class SmvResultTest(SmvBaseTest):
    @classmethod
    def smvAppInitArgs(cls):
        return ["--smv-props", "smv.stages=stage1:stage2"]

    def test_SmvResultModule_persistence(self):
        """Test persistence of non-DataFrame results
        """
        with ExtraPath("src/test/python/smv_result"):
            res = self.smvApp.getModuleResult("stage1.modules.RM")
        self.assertEqual(res, [100, "100", 100.0])

    def test_link_to_SmvResultModule(self):
        """Test that result of link to module with non-DataFrame result same as module's result
        """
        with ExtraPath("src/test/python/smv_result"):
            RMres = self.smvApp.getModuleResult("stage2.modules.RM")
            Mdf = self.smvApp.getModuleResult("stage2.modules.M")
        self.assertEqual(str(RMres), Mdf.collect()[0][0])
