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
from smv.iomod import SmvCsvOutputFile

import testSmvOutput

class MyData(SmvModule):
    def requiresDS(self):
        return []

    def run(self, i):
        return self.smvApp.createDF("a:String", "1")

class CsvOut(SmvCsvOutputFile):
    def connectionName(self):
        return "my_out_conn"

    def fileName(self):
        return "csv_out_test.csv"

    def requiresDS(self):
        return [MyData]


class CsvOutRerun(SmvCsvOutputFile):
    def connectionName(self):
        return "my_out_conn"

    def fileName(self):
        return "csv_out_test2.csv"

    def requiresDS(self):
        return [MyData]

    def doRun(self, known):
        testSmvOutput.output_run_cnt += 1
        return super(CsvOutRerun, self).doRun(known)

