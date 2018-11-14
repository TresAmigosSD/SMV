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
from smv.functions import smvStrCat
from smv.iomod import SmvCsvInputFile

import pyspark.sql.functions as F

class D1(SmvCsvStringData):
    def schemaStr(self):
        return "a:String;b:Integer"
    def dataStr(self):
        return "x,10;y,1"

class D1WithError(SmvCsvStringData):
    def failAtParsingError(self):
        return False
    def dqm(self):
        return SmvDQM().add(FailParserCountPolicy(1))

    def schemaStr(self):
        return "a:String;b:Integer"
    def dataStr(self):
        return "a1,10;a2,x;a3,2;a4,;a5,y"

class MultiCsv(SmvMultiCsvFiles):
    def dir(self):
        return "multiCsvTest"

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


class Xml1(SmvXmlFile):
    def fullPath(self):
        return self.smvApp.inputDir() + '/' + 'xmltest/f1.xml'
    def fullSchemaPath(self):
        return None
    def rowTag(self):
        return 'ROW'

class Xml2(SmvXmlFile):
    def fullPath(self):
        return self.smvApp.inputDir() + '/' + 'xmltest/f1.xml'
    def fullSchemaPath(self):
        return self.smvApp.inputDir() + '/' + 'xmltest/f1.xml.json'
    def rowTag(self):
        return 'ROW'

class Xml3(SmvXmlFile):
    def path(self):
        return 'xmltest/f1.xml'
    def rowTag(self):
        return 'ROW'

class IFF1(SmvInputFromFile):
    def path(self):
        return "xmltest/f1.xml.gz"
    def readAsDF(self):
        pass

class Csv1(SmvCsvFile):
    def path(self):
        return "csvtest/csv1.csv"
    def csvAttr(self):
        return CsvAttributes(",", '"', True)
    def run(self, df):
        return df.withColumn("name_id",
            smvStrCat(F.col("name"), F.col("id"))
        )

class Csv2(SmvCsvFile):
    def path(self):
        return "csvtest/csv1.csv"
    def csvAttr(self):
        return CsvAttributes(",", '"', True)
    def userSchema(self):
        return "eman:String;di:integer"


class NewCsvFile1(SmvCsvInputFile):
    def connectionName(self):
        return "my_hdfs"

    def fileName(self):
        return "csvtest/csv1.csv"