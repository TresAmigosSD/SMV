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
from fixture.hive.modules import M, MyHive, MyHiveWithQuery

class HiveTest(SmvBaseTest):
    @classmethod
    def smvAppInitArgs(cls):
        return ['--smv-props', 'smv.stages=fixture.hive']

    @classmethod
    def setUpClass(cls):
        super(HiveTest, cls).setUpClass()
        cls.smvApp.sqlContext.setConf("hive.metastore.warehouse.dir", "file:///tmp/smv_hive_test")

# temporarily turn off the tests in this file. since we can't figure out
# a way to specify the temp hive storage conf in 2.1. Specify
# spark.sql.warehouse.dir doesn't solve the problem. The only way to
# make the tests pass is to create /user/warehouse/m dir on the building machine
class PublishModuleToHiveTest(HiveTest):
    @classmethod
    def smvAppInitArgs(cls):
        return super(PublishModuleToHiveTest, cls).smvAppInitArgs() + ['--publish-hive', '-m', M.fqn()]

    def test_publish_module_to_hive(self):
        self.smvApp.j_smvApp.run()
        mDf = self.smvApp.runModule(M.urn())
        hiveDf = self.smvApp.sqlContext.sql("select * from " + "M")
        self.should_be_same(mDf, hiveDf)

class ReadHiveTableTest(HiveTest):
    @classmethod
    def smvAppInitArgs(cls):
        return super(ReadHiveTableTest, cls).smvAppInitArgs() + ['--publish-hive', '-m', M.fqn()]

    @classmethod
    def setUpClass(cls):
        super(ReadHiveTableTest, cls).setUpClass()
        cls.smvApp.j_smvApp.run()

    def test_smv_hive_table_can_read_hive_table(self):
        mDf = self.smvApp.runModule(M.urn())
        hiveDf = self.smvApp.runModule(MyHive.urn())
        self.should_be_same(mDf,hiveDf)

    def test_smv_hive_table_can_use_custom_query(self):
        mDf = self.smvApp.runModule(M.urn()).select("k")
        hiveDf = self.smvApp.runModule(MyHiveWithQuery.urn())
        self.should_be_same(mDf,hiveDf)
