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
import json

from test_support.smvbasetest import SmvBaseTest
from smv import *
from smv.datasetrepo import DataSetRepo

class SmvInputTest(SmvBaseTest):
    @classmethod
    def smvAppInitArgs(cls):
        return ['--smv-props', 'smv.stages=stage']

    class Resource(object):
        def __init__(self, smvApp, fqn):
            self.dsr = DataSetRepo(smvApp)
            self.fqn = fqn

        def __enter__(self):
            return self.dsr.loadDataSet(self.fqn)

        def __exit__(self, type, value, traceback):
            pass

    def _create_xml_file(self):
        self.createTempInputFile("xmltest/f1.xml",
        """<?xml version="1.0"?>
<ROWSET>
    <ROW>
        <year>2012<!--A comment within tags--></year>
        <make>Tesla</make>
        <model>S</model>
        <comment>No comment</comment>
    </ROW>
    <ROW>
        <year>1997</year><!--A comment within elements-->
        <make>Ford</make>
        <model>E350</model>
        <comment><!--A comment before a value-->Go get one now they are going fast</comment>
    </ROW>
</ROWSET>""")
        self.createTempInputFile("xmltest/f1.xml.json",
        """{
  "fields": [
    {
      "metadata": {},
      "name": "comment",
      "nullable": true,
      "type": "string"
    },
    {
      "metadata": {},
      "name": "make",
      "nullable": true,
      "type": "string"
    },
    {
      "metadata": {},
      "name": "model",
      "nullable": true,
      "type": "string"
    },
    {
      "metadata": {},
      "name": "year",
      "nullable": true,
      "type": "long"
    }
  ],
  "type": "struct"
}""")

    def _schema_json_str(self, schema):
        import json
        sch_str = json.dumps(schema.jsonValue(), indent=2, separators=(',', ': '), sort_keys=True)
        return sch_str

    def _dump_schema(self, schema, jsonfile):
        o = file(jsonfile, "w+")
        o.write(self._schema_json_str(schema))
        o.close()

    def test_SmvXmvFile_infer_schema(self):
        fqn = "stage.modules.Xml1"
        self._create_xml_file()
        df = self.df(fqn)
        expect = self.createDF("comment: String;make: String;model: String;year: Long",
            """No comment,Tesla,S,2012;
                Go get one now they are going fast,Ford,E350,1997""")
        self.should_be_same(expect, df)

    def test_SmvXmvFile_given_schema(self):
        fqn = "stage.modules.Xml2"
        self._create_xml_file()
        df = self.df(fqn)
        expect = self.createDF("comment: String;make: String;model: String;year: Long",
            """No comment,Tesla,S,2012;
                Go get one now they are going fast,Ford,E350,1997""")
        self.should_be_same(expect, df)

    def test_SmvInputFromFile_instanceValHash_mtime(self):
        fqn = "stage.modules.Xml1"
        self._create_xml_file()
        with self.Resource(self.smvApp, fqn) as ds:
            hash1 = ds.instanceValHash()

        # Need to sleep to make sure mtime changes
        import time
        time.sleep(1)
        self._create_xml_file()
        with self.Resource(self.smvApp, fqn) as ds:
            hash2 = ds.instanceValHash()
        self.assertNotEqual(hash1, hash2)

    def test_SmvInputFromFile_schemaPath(self):
        fqn = "stage.modules.IFF1"
        with self.Resource(self.smvApp, fqn) as ds:
            spath = ds.fullSchemaPath()
        expected = self.tmpInputDir() + "/xmltest/f1.schema"
        self.assertEqual(spath, expected)