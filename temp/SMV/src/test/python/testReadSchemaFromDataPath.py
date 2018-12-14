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
from smv import SmvApp
import os

class ReadSchemaFromDataPathTest(SmvBaseTest):
    def test_readSchemaWhenFileExist(self):
        cls = self.__class__
        app = SmvApp.getInstance()
        schema_file_name = "schemaToBeRead1.schema"
        schema_file_path = os.path.join(cls.tmpInputDir(), schema_file_name)
        schema_file_content = ('@delimiter = ,\n'
                               '@has-header = true\n'
                               '@quote-char = "\n'
                               'a: String\n'
                               'b: Integer')

        self.createTempInputFile(schema_file_name, schema_file_content)

        data_file_path = schema_file_path.replace(".schema", ".csv")
        smv_schema_instance = app.j_smvPyClient.readSchemaFromDataPathAsSmvSchema(data_file_path)

        entries = smv_schema_instance.getEntriesStr()
        attributes = smv_schema_instance.extractCsvAttributes()

        self.assertEqual(len(entries), 2)
        self.assertEqual(entries[0], 'a: String')
        self.assertEqual(entries[1], 'b: Integer')

        self.assertTrue(attributes.hasHeader())
        self.assertEqual(attributes.delimiter(), ',')
        self.assertEqual(attributes.quotechar(), '"')

    def test_readSchemaWhenFileNotExist(self):
        cls = self.__class__
        app = SmvApp.getInstance()
        schema_file_name = "schemaToBeRead2.schema"
        schema_file_path = os.path.join(cls.tmpInputDir(), schema_file_name)

        data_file_path = schema_file_path.replace(".schema", ".csv")
        smv_schema_instance = app.j_smvPyClient.readSchemaFromDataPathAsSmvSchema(data_file_path)

        self.assertIsNone(smv_schema_instance)
