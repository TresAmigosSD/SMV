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

from smv import SmvApp, SmvModule, SmvHiveTable, SmvCsvFile

class ChangeCode(SmvModule):
    def requiresDS(self):
        return []
    def run(self, i):
        return self.smvApp.createDF("k:String;v:Integer", "a,;b,3")

class AddComment(SmvModule):
    # I ADDED A COMMENT, POP!
    def requiresDS(self):
        return []
    def run(self,i):
        return self.smvApp.createDF("k:String;v:Integer", "a,;b,5")

class DependencyB(SmvModule):
    def requiresDS(self):
        return []
    def run(self,i):
        return self.smvApp.createDF("k:String;v:Integer", "a,;b,215")

class Dependent(DependencyB):
    def requiresDS(self):
        return []
    def run(self,i):
        return self.smvApp.createDF("k:String;v:Integer", "a,;b,7")

class Upstream(SmvModule):
    def requiresDS(self):
        return []
    def run(self,i):
        return self.smvApp.createDF("k:String;v:Integer", "a,;b,46")

class Downstream(SmvModule):
    def requiresDS(self):
        return[Upstream]
    def run(self,i):
        return self.smvApp.createDF("k:String;v:Integer", "a,;b,30")

class Parent(SmvModule):
    def requiresDS(self):
        return[Upstream]
    def run(self,i):
        return self.smvApp.createDF("k:String;v:Integer", "a,;b,31")

class HiveTableWithVersion(SmvHiveTable):
    def requiresDS(self):
        return []
    def tableName(self):
        return "HiveTableWithVersion"
    def version(self):
        return "1.1"

class CsvFileWithRun(SmvCsvFile):
    def requiresDS(self):
        return []
    def path(self):
        return "foo"
    def run(self, df):
        return df.select("bar")

class Child(Parent):
    pass

class UsesConfigValue(SmvModule):
    def requiresDS(self):
        return[]
    def run(self, i):
        pass
    def requiresConfig(self):
        return ["keyChanges"]

class DoesntConfigValue(SmvModule):
    def requiresDS(self):
        return []
    def run(self, i):
        pass
    def requiresConfig(self):
        return ["keyDoesntChange"]
