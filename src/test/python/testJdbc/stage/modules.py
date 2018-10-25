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

class MyJdbcTable(SmvJdbcTable):
    def tableName(self):
        return "MyJdbcTable"

class MyJdbcWithQuery(SmvJdbcTable):
    def tableQuery(self):
        return "select K from MyJdbcTable"
    def tableName(self):
        return "MyJdbcTable"

class MyJdbcCsvString(SmvCsvStringData, SmvOutput):
    def tableName(self):
        return "MyJdbcOutput"

    def schemaStr(self):
        return "a:String;b:Integer"
    def dataStr(self):
        return "x,10;y,1"

class MyJdbcModule(SmvModule):
    def requiresDS(self):
        return []

    def tableName(self):
        return "MyJdbcModule"

    def run(self, i):
        return self.smvApp.createDF("a:String", "1")