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

from smv import SmvResultModule

class SmvResultTest(SmvBaseTest):
    @classmethod
    def smvAppInitArgs(cls):
        return ["--smv-props", "smv.stages=stage"]

    def test_SmvResultModule(self):
        """Test serialization/deserialization of
        """
        with ExtraPath("src/test/python/smv_result"):
            resDf = self.smvApp.runModule("mod:stage.modules.RM")
        res = SmvResultModule.df2result(resDf)
        self.assertEqual(res, [100, "100", 100.0])
