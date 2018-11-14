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

import os
from smv.smvshell import openCsv
from test_support.smvbasetest import SmvBaseTest

class SmvOutputTest(SmvBaseTest):
    @classmethod
    def smvAppInitArgs(cls):
        data_path = cls.tmpDataDir()
        return [
            "--smv-props", 
            "smv.stages=stage",
            "smv.conn.my_out_conn.class=smv.conn.SmvHdfsConnectionInfo",
            "smv.conn.my_out_conn.path=" + data_path,
        ]

    def test_csv_out_basic(self):
        res = self.df("stage.modules.CsvOut")
        self.assertTrue(os.path.exists(self.tmpDataDir() + "/csv_out_test.csv"))
        self.assertTrue(os.path.exists(self.tmpDataDir() + "/csv_out_test.schema"))

        read_back = openCsv(self.tmpDataDir() + "/csv_out_test.csv")

        self.should_be_same(res, read_back)