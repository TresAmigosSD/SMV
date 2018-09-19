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
import udl as lib
import same as unchanged
from smv import SmvApp, SmvModule, SmvHiveTable, SmvCsvFile

def sameFunc():
    """a function which is the same in before/after"""
    return "I'm the same!"

def differentFunc():
    """a function that has different source in before/after"""
    return "I'm in after!"

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

class DifferentLibrary(SmvModule):
    def requiresDS(self):
        return []
    def requiresLib(self):
        return [lib]
    def run(self, i):
        number = lib.getNumber()
        return self.smvApp.createDF("k:String;v:Integer", "a,1;b,2;c:3").limit(number)

class SameLibrary(SmvModule):
    def requiresDS(self):
        return []
    def requiresLib(self):
        return [unchanged]
    def run(self, i):
        df = self.smvApp.createDF("k:String;v:Integer", "a,1;b,2;c:3")
        return df.withColumn('new', unchanged.UnchangedFunction())

class SameFunc(SmvModule):
    def requiresDS(self):
        return []
    def requiresLib(self):
        return [sameFunc]
    def run(self, i):
        df = self.smvApp.createDF("k:String;v:Integer", "a,1;b,2;c:3")
        return df.withColumn('new', sameFunc())


class DifferentFunc(SmvModule):
    def requiresDS(self):
        return []
    def requiresLib(self):
        return [differentFunc]
    def run(self, i):
        df = self.smvApp.createDF("k:String;v:Integer", "a,1;b,2;c:3")
        return df.withColumn('new', differentFunc())

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

class CsvFileWithAttr(SmvCsvFile):
    def path(self):
        return "foo"
    def userSchema(self):
        return "@quote-char=';eman:String;di:integer"

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
