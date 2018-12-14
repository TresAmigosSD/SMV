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

from smv import SmvApp, SmvModule, SmvRunConfig

class A(SmvModule, SmvRunConfig):
    def isEphemeral(self): return True
    def requiresDS(self): return []
    def requiresConfig(self): return ["src"]
    def run(self, i):
        return self.smvApp.createDF("src:String", self.smvGetRunConfig("src"))


class RunConfWithError(SmvModule):
    def requiresDS(self): return []
    def run(self, i):
        # Using "src" without defining requiresConfig, should error out
        return self.smvApp.createDF("src:String", self.smvGetRunConfig("src"))
