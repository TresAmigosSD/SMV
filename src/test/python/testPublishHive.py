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

from smvbasetest import SmvBaseTest

class PublishModuleToHiveTest(SmvBaseTest):
    urn = "mod:fixture.hive.modules.M"

    @classmethod
    def smvAppInitArgs(cls):
        return ['--smv-props', 'smv.stages=fixture.hive', '--publish-hive',
                '-m', 'fixture.hive.modules.M']

    def test_publish_module_to_hive(self):
        self.smvPy.sqlContext.setConf("hive.metastore.warehouse.dir", "file:///tmp/Z")
        self.smvPy.j_smvApp.run()
        Mdf = self.smvPy.runModule(self.urn)
        MdfHive = self.smvPy.sqlContext.sql("select * from " + "M")
        self.should_be_same(Mdf, MdfHive)

class ReadHiveTableTest(SmvBaseTest):

    @classmethod
    def smvAppInitArgs(cls):
        return ['--smv-props', 'smv.stages=fixture.hive', '--publish-hive',
                '-m', 'fixture.hive.modules.M']

    def setUp(self):
        super(ReadHiveTableTest,self).setUp()
        self.smvPy.sqlContext.setConf("hive.metastore.warehouse.dir", "file:///tmp/Z")
        self.smvPy.j_smvApp.run()

    def test_smv_hive_table_can_read_hive_table(self):
        Mdf = self.smvPy.runModule("mod:fixture.hive.modules.M")
        HiveDf = self.smvPy.runModule("mod:fixture.hive.modules.MyHive")
        self.should_be_same(Mdf,HiveDf)

    def test_smv_hive_table_can_use_custom_query(self):
        Mdf = self.smvPy.runModule("mod:fixture.hive.modules.M").select("k")
        HiveDf = self.smvPy.runModule("mod:fixture.hive.modules.MyHiveWithQuery")
        self.should_be_same(Mdf,HiveDf)
