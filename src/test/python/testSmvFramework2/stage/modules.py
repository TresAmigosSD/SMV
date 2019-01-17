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

import testSmvFramework2

import time

class I1(SmvModule):
    def requiresDS(self):
        return []
    def run(self, i):
        return self.smvApp.createDF(
            "a:Integer;b:Double",
            "1,0.3;0,0.2;3,0.5")

class M1(SmvModule):
    def requiresDS(self):
        return [I1]
    def isEphemeral(self):
        return True
    def run(self, i):
        testSmvFramework2.cross_run_counter += 1
        return i[I1]
    def _post_action(self):
        super(M1, self)._post_action()
        testSmvFramework2.m1_post_counter += 1

class M2(SmvModule):
    def requiresDS(self):
        return [M1]
    def run(self, i):
        testSmvFramework2.persist_run_counter += 1
        return i[M1]
    def dqm(self):
        return SmvDQM().add(
            DQMRule(col("b") < 0.4 , "b_lt_04")).add(
            FailTotalRuleCountPolicy(2))

class M3(SmvModule):
    def requiresDS(self):
        return [M1, M2]
    def isEphemeral(self):
        return True
    def run(self, i):
        return i[M1]
    def metadata(self, df):
        return {'n':df.count()}

class M5(SmvModule):
    def requiresDS(self):
        return [M2]
    def run(self, i):
        return i[M2]


class WithLib(SmvModule):
    def requiresDS(self):
        return [M2]
    def requiresLib(self):
        return [time]
    def run(self, i):
        return i[M2]

class WrongType(SmvModule):
    def requiresDS(self):
        return [M2]
    def run(self, i):
        return ['a', 'b', 'c']
