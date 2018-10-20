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
from smv.dqm import *
from pyspark.sql.functions import col, lit

class I1(SmvCsvStringData):
    def schemaStr(self):
        return "a:Integer;b:Double"
    def dataStr(self):
        return "1,0.3;0,0.2;3,0.5"

class M1(SmvModule):
    def requiresDS(self):
        return [I1]
    def isEphemeral(self):
        return True
    def run(self, i):
        return i[I1]

class M2(SmvModule):
    def requiresDS(self):
        return [M1]
    def run(self, i):
        return i[M1]

class M3(SmvModule):
    def requiresDS(self):
        return [M1, M2]
    def isEphemeral(self):
        return True
    def run(self, i):
        return i[M1]