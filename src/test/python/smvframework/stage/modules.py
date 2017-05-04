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

# from smv import SmvApp, SmvModule, SmvOutput
from smv import *
from smv.dqm import *
from pyspark.sql.functions import col, lit


class D1(SmvPyCsvStringData):
    def schemaStr(self):
        return "a:String;b:Integer"
    def dataStr(self):
        return "x,10;y,1"

class D2(SmvPyMultiCsvFiles):
    def dir(self):
        return "test3"

class D3(SmvPyCsvStringData):
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

class D4(SmvPyCsvStringData, SmvRunConfig):
    def schemaStr(self):
        return "a:String;b:Integer"
    def dataStr(self):
        if(self.smvGetRunConfig('s') == "s1"):
            return "a,10;b,1"
        else:
            return "X,100;Y,200;"
