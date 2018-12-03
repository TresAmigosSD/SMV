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

class D4(SmvModule, SmvRunConfig):
    def schemaStr(self):
        return "a:String;b:Integer"
    def dataStr(self):
        testVals = [];
        if (self.smvGetRunConfig('s') == "s1"):
            testVals.append("test1_s1,1")
        else:
            testVals.append("test1_not_s1,2")

        if (self.smvGetRunConfigAsInt('i') == 2):
            testVals.append("test2_i2,3")
        else:
            testVals.append("test2_not_i2,4")

        if (self.smvGetRunConfigAsBool('b')):
            testVals.append("test3_b,5")
        else:
            testVals.append("test3_not_b,6")

        # Tests to make sure that if the runconfig doesn't exist, None is returned
        if (self.smvGetRunConfig('c') is None):
            testVals.append("test4_undefined_c,7")
        else:
            testVals.append("test4_defined_c,8")

        if (self.smvGetRunConfigAsInt('one') is None):
            testVals.append("test5_undefined_one,9")
        else:
            testVals.append("test5_defined_one,10")

        if (self.smvGetRunConfigAsBool('bool') is None):
            testVals.append("test6_undefined_bool,11")
        else:
            testVals.append("test6_defined_bool,12")

        return ";".join(testVals)

    def requiresDS(self):
        return []
    def run(self, i):
        return self.smvApp.createDF(self.schemaStr(), self.dataStr())

class CsvFile(SmvCsvFile, SmvOutput):
    def path(self):
        return "test3.csv"

class CsvStrWithNullData(SmvCsvStringData):
    def schemaStr(self):
        return "a:String"

    def dataStr(self):
        return "1;;3"

    def isEphemeral(self):
        return False

    def run(self, df):
        return df.withColumn("b", lit(""))

class ModWithBadName(SmvModule):
    def requiresDS(self):
        return [ModWhoseNameDoesntExist]

    def run(self, i):
        return self.smvApp.createDF("b: String", "xxx")

class NeedRunM1(SmvModule):
    def requiresDS(self):
        return []

    def run(self, i):
        return self.smvApp.createDF("ida: Integer; a: String", "1,def;2,ghi")

class NeedRunM2(SmvModule):
    def requiresDS(self):
        return [NeedRunM1]

    def run(self, i):
        return i[NeedRunM1]

class NeedRunM3(SmvModule):
    def requiresDS(self):
        return [NeedRunM1]

    def isEphemeral(self):
        return True

    def run(self, i):
        return i[NeedRunM1]
