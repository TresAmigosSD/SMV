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

from smv import SmvApp, SmvModule, SmvOutput, SmvHiveTable
from smv.iomod import SmvHiveInputTable, SmvHiveOutputTable

class M(SmvModule, SmvOutput):
    def requiresDS(self): return []
    def tableName(self): return "M"
    def run(self, i):
        return self.smvApp.createDF("k:String;v:Integer", "a,;b,2")

class MAdv(SmvModule, SmvOutput):
    """Advanced Hive publish module that uses the publishHiveSql method to override
       the output of table M in hive."""
    def requiresDS(self): return []
    def publishHiveSql(self): return "INSERT OVERWRITE TABLE M SELECT * from dftable"
    def run(self, i):
        return self.smvApp.createDF("k:String;v:Integer", "x,1;y,2")

class MyHive(SmvHiveTable):
    def tableName(self): return "M"


class NewHiveInput(SmvHiveInputTable):
    def tableName(self): return "M"
    def connectionName(self): return "my_hive"

class NewHiveOutput(SmvHiveOutputTable):
    def requiresDS(self):
        return [NewHiveInput]
    def tableName(self): return "WriteOutM"
    def connectionName(self): return "my_hive"
