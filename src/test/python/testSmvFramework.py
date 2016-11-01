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

import unittest

from smvbasetest import SmvBaseTest
from smv import SmvPyCsvStringData

import pyspark
from pyspark.context import SparkContext
from pyspark.sql import SQLContext, HiveContext
from pyspark.sql.functions import col, struct

class D1(SmvPyCsvStringData):
    def schemaStr(self):
        return "a:String;b:Integer"
    def dataStr(self):
        return "x,10;y,1"

class SmvFrameworkTest(SmvBaseTest):
    def test_SmvCsvStringData(self):
        fqn = self.__module__ + ".D1"
        df = self.smv.runModule(fqn)
        expect = self.createDF("a:String;b:Integer", "x,10;y,1")
        self.should_be_same(expect, df)
