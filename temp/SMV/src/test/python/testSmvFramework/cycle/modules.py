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

from smv import *
import testSmvFramework

class CycleA(SmvModule):
    def requiresDS(self):
        return [CycleB]

    def run(self, i):
        return None

class CycleB(SmvModule):
    def requiresDS(self):
        return [CycleA]

    def run(self, i):
        return None

class SingleRunA(SmvModule):
    def requiresDS(self):
        return []

    def run(self, i):
        testSmvFramework.single_run_counter = testSmvFramework.single_run_counter + 1
        return self.smvApp.createDF("cnt:Integer", "0")

class SingleRunB(SmvModule):
    def requiresDS(self):
        return [SingleRunA]
    def run(self, i):
        return i[SingleRunA]

class SingleRunC(SmvModule):
    def requiresDS(self):
        return [SingleRunA]
    def run(self, i):
        return i[SingleRunA]
