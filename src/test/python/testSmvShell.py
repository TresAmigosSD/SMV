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

from test_support.smvbasetest import SmvBaseTest
import smv.smvshell as smvshell
import os
import json

class SmvShellTest(SmvBaseTest):
    @classmethod
    def smvAppInitArgs(cls):
        return ['--smv-props', 'smv.stages=stage:stage2']

    def test_shell_df(self):
        fqn = "stage.modules.CsvStr"
        res = smvshell.df(fqn)
        expected = self.createDF(
            "name:String;id:integer",
            "bob,1"
        )
        self.should_be_same(res, expected)

    def test_shell_cmds(self):
        cmd = smvshell._jvmShellCmd()

        self.assertEqual(cmd.ls(),
            """
stage:
  (I) stage.modules.CsvStr
  (O) stage.modules.M1

stage2:
  (O) stage.modules.M1
  (I) stage2.modules.CsvStr2
  (M) stage2.modules.M2""")
        self.assertEqual(cmd.lsStage(),
        """stage
stage2""")

        self.assertEqual(cmd.lsDead(),
        """
stage:

stage2:
  (O) stage.modules.M1
  (I) stage2.modules.CsvStr2
  (M) stage2.modules.M2""")
        
        self.assertEqual(cmd.ancestors("M2"),
        """(I) stage2.modules.CsvStr2
(O) stage.modules.M1
(I) stage.modules.CsvStr""")

        self.assertEqual(cmd.descendants("CsvStr"),
        """(O) stage.modules.M1
(M) stage2.modules.M2""")

    def test_smvDiscoverSchemaToFile(self):
        file_name = "discoverSchema.csv"
        out_schema_name = "discoverSchema.schema.toBeReviewed"
        self.createTempInputFile(file_name,
            "name,id\nbob,1"
        )
        smvshell.smvDiscoverSchemaToFile(self.tmpInputDir() + "/" + file_name)
        assert os.path.exists(out_schema_name)
        os.remove(out_schema_name)

    def test_app_createDF_to_create_empty_df(self):
        res = self.smvApp.createDF("a:String")
        self.assertEqual(res.count(), 0)

    def test_app_getFileNamesByType(self):
        self.createTempInputFile("check_file.csv")
        self.createTempInputFile("check_file.schema")
        self.assertEqual(self.smvApp.getFileNamesByType("csv"), ['check_file.csv'])

    def test_app_getMetadataJson(self):
        fqn = "stage.modules.CsvStr"
        self.df(fqn)
        meta = json.loads(self.smvApp.getMetadataJson("mod:" + fqn))
        self.assertEqual(meta['_fqn'], fqn)

    def test_app_getMetadataHistoryJson(self):
        fqn = "stage.modules.CsvStr"
        self.df(fqn)
        metahist = json.loads(self.smvApp.getMetadataHistoryJson("mod:" + fqn))
        self.assertEqual(metahist['history'][0]['_fqn'], fqn)

    def test_app_getDsHash(self):
        fqn = "stage.modules.CsvStr"
        print(self.smvApp.getDsHash(fqn, None))
