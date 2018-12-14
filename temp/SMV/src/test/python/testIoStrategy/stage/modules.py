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
from smv.smviostrategy import SmvParquetPersistenceStrategy

class M1(SmvModule):
    def requiresDS(self):
        return []
    def run(self, i):
        return self.smvApp.createDF("k:String;v:Integer", "a,1;b,2")

    def persistStrategy(self):
        return SmvParquetPersistenceStrategy(self.smvApp, self.versioned_fqn)

class M2(SmvModule):
    def requiresDS(self):
        return [M1]
    def run(self, i):
        return i[M1]
