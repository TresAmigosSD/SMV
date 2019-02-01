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
from pyspark.sql import DataFrame
from smv.smvmodulerunner import SmvModuleRunner


class NewJdbcTest(SmvBaseTest):
    @classmethod
    def setUpClass(cls):
        super(NewJdbcTest, cls).setUpClass()
        cls.smvApp._jvm.org.tresamigos.smv.jdbc.JdbcDialectHelper.registerDerby()

    @classmethod
    def url(cls):
        return "jdbc:derby:" + cls.tmpTestDir() + "/derby;create=true"

    @classmethod
    def driver(cls):
        return "org.apache.derby.jdbc.EmbeddedDriver"

    @classmethod
    def smvAppInitArgs(cls):
        return [
            "--smv-props",
            "smv.stages=stage",
            "smv.conn.myjdbc_conn.type=jdbc",
            "smv.conn.myjdbc_conn.url=" + cls.url(),
            "smv.conn.myjdbc_conn.driver=" + cls.driver()
        ]

    def test_SmvJdbcInputTable(self):
        df = self.createDF("K:String", "xxx")
        df.write.jdbc(self.url(), "MyJdbcTable", properties={"driver": "org.apache.derby.jdbc.EmbeddedDriver"})
        res = self.df("stage.modules.NewJdbcTable")
        self.should_be_same(res, df)


    def test_SmvJdbcOutputTable(self):
        df = self.df("stage.modules.NewJdbcOutputTable")
        res = self.smvApp.sqlContext.read\
            .format("jdbc")\
            .option("url", self.url())\
            .option("dbtable", "MyJdbcTable")\
            .load()
        self.should_be_same(res, df)

# Comment out this test until jdbc's get_contents implemented
#    def test_conn_contents(self):
#        conn = self.smvApp.get_connection_by_name('myjdbc_conn')
#        res = [f.lower() for f in conn.get_contents(self.smvApp)]
#        self.assertTrue('MyJdbcTable'.lower() in res)
