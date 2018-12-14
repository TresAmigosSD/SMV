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

class DiscoverSchemaTest(SmvBaseTest):
    def test_discoverSchema(self):
        cls = self.__class__
        from smv.smvshell import smvDiscoverSchemaToFile
        import os

        self.createTempInputFile("schemaToBeDiscovered.csv", 'a,b,c\n1,2,"a"\n3,a,"f"\n')
        smvDiscoverSchemaToFile(cls.tmpInputDir() + "/schemaToBeDiscovered.csv")

        sf = open("schemaToBeDiscovered.schema.toBeReviewed", "r")
        res = sf.read()
        sf.close()
        os.remove("schemaToBeDiscovered.schema.toBeReviewed")

        exp = ('@delimiter = ,\n'
               '@has-header = true\n'
               '@quote-char = "\n'
               'a: Integer @metadata={"smvDesc":"1"}\n'
               'b: String @metadata={"smvDesc":"2"}\n'
               'c: String @metadata={"smvDesc":"a"}')

        self.assertEqual(res, exp)
