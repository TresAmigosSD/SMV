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

output_run_cnt = 0

class SmvOutputTest(SmvBaseTest):
    @classmethod
    def smvAppInitArgs(cls):
        data_path = cls.tmpDataDir()
        return [
            "--smv-props",
            "smv.stages=stage",
            "smv.conn.my_out_conn.type=hdfs",
            "smv.conn.my_out_conn.path=" + data_path,
        ]

    def test_csv_out_basic(self):
        res = self.df("stage.modules.CsvOut")
        self.assertTrue(os.path.exists(self.tmpDataDir() + "/csv_out_test.csv"))
        self.assertTrue(os.path.exists(self.tmpDataDir() + "/csv_out_test.schema"))

        read_back = openCsv(self.tmpDataDir() + "/csv_out_test.csv")

        self.should_be_same(res, read_back)

    def test_csv_out_overwrite_by_default(self):
        file_base = self.tmpDataDir() + "/csv_out_test"

        # create files if they are not exists
        if (not os.path.exists(file_base + ".csv")):
            open(file_base + ".csv", "a").close()
        if (not os.path.exists(file_base + ".schema")):
            open(file_base + ".schema", "a").close()

        # run output should overwrite
        res = self.df("stage.modules.CsvOut")
        read_back = openCsv(file_base + ".csv")
        self.should_be_same(res, read_back)

    def test_csv_out_run_each_time(self):
        r1 = self.df("stage.modules.CsvOutRerun")
        r2 = self.df("stage.modules.CsvOutRerun")
        global output_run_cnt
        self.assertEqual(output_run_cnt, 2)
