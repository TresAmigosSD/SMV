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
from smv.iomod import SmvCsvInputFile
from smv.conn import SmvHdfsEmptyConn
from pyspark.sql.functions import col, lit

class D1(SmvCsvStringData):
    def schemaStr(self):
        return "k:String;v:Integer"
    def dataStr(self):
        return "a,1;b,2"

class T(SmvCsvInputFile):
    def connectionName(self):
        return None
    def get_connection(self):
        return SmvHdfsEmptyConn
    def fileName(self):
        return "./target/python-test-export-csv.csv"
    def csvAttr(self):
        return self.smvApp.defaultCsvWithHeader()

class X(SmvModule):
    def isEphemeral(self): return True
    def requiresDS(self): return []
    def run(self, i):
        return self.smvApp.createDF("""k:String; t:Integer @metadata={"smvDesc":"the time sequence"}; v:Double""",
            "z,1,0.2;z,2,1.4;z,5,2.2;a,1,0.3;")
