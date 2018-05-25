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

class D1(SmvCsvStringData):
    def schemaStr(self):
        return "a:String;b:Integer"
    def dataStr(self):
        return "x,10;y,1"

class D1WithError(SmvCsvStringData):
    def schemaStr(self):
        return "a:String;b:Integer"
    def dataStr(self):
        return "a1,10;a2,x;a3,2;a4,;a5,y"

class MultiCsv(SmvMultiCsvFiles):
    def dir(self):
        return "multiCsvTest"

class D3(SmvCsvStringData):
    def schemaStr(self):
        return "a:Integer;b:Double"
    def dataStr(self):
        return "1,0.3;0,0.2;3,0.5"
    def dqm(self):
        return SmvDQM().add(
            DQMRule(col("b") < 0.4 , "b_lt_03")).add(
            DQMFix(col("a") < 1, lit(1).alias("a"), "a_lt_1_fix")).add(
            FailTotalRuleCountPolicy(2)).add(
            FailTotalFixCountPolicy(1))

class D4(SmvCsvStringData, SmvRunConfig):
    def schemaStr(self):
        return "a:String;b:Integer"
    def dataStr(self):
        if(self.smvGetRunConfig('s') == "s1"):
            l1 = "test1_s1,1"
        else:
            l1 = "test1_not_s1,2"

        if(self.smvGetRunConfigAsInt('i') == 2):
            l2 = "test2_i2,3"
        else:
            l2 = "test2_not_i2,4"

        if(self.smvGetRunConfigAsBool('b')):
            l3 = "test3_b,5"
        else:
            l3 = "test3_not_b,6"

        # Tests to make sure that if the runconfig doesn't exist, None is returned
        if(self.smvGetRunConfig('c') is None):
            l4 = "test4_c,7"
        else:
            l4 = "test4_not_c,8"

        if(self.smvGetRunConfigAsInt('c') is None):
            l5 = "test5_c,9"
        else:
            l5 = "test5_not_c,10"

        if(self.smvGetRunConfigAsBool('c') is None):
            l6 = "test6_c,11"
        else:
            l6 = "test6_not_c,12"

        return l1 + ";" + l2 + ";" + l3 + ";" + l4 + ";" + l5 + ";" + l6

class MultiCsvWithUserSchema(SmvMultiCsvFiles):
    UserSchema = "1loc: String"

    def dir(self):
        return "test3"

    def userSchema(self):
        return self.UserSchema

class CsvFile(SmvCsvFile):
    UserSchema = "1loc: String"

    def path(self):
        return "test3.csv"

    def userSchema(self):
        return self.UserSchema

class SqlCsvFile(SmvSqlCsvFile):
    UserSchema = "a: String; b: Integer; c: String"

    def path(self):
        return "test3.csv"

    def userSchema(self):
        return self.UserSchema

    def query(self):
        return "select a, b from df"

class SqlMod(SmvSqlModule):
    def tables(self):
        return {
            "A": SqlInputA,
            "B": SqlInputB
        }

    def query(self):
        return "select a, b from A inner join B on A.ida = B.idb"

class SqlInputA(SmvModule):
    def requiresDS(self):
        return []

    def run(self, i):
        return self.smvApp.createDF("ida: Integer; a: String", "1,def;2,ghi")

class SqlInputB(SmvModule):
    def requiresDS(self):
        return []

    def run(self, i):
        return self.smvApp.createDF("idb: Integer; b: String", "2,jkl;1,mno")

class ModWithBadName(SmvModule):
    def requiresDS(self):
        return [ModWhoseNameDoesntExist]

    def run(self, i):
        return self.smvApp.createDF("b: String", "xxx")
