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

class JdbcTest(SmvBaseTest):
    @classmethod
    def setUpClass(cls):
        super(JdbcTest, cls).setUpClass()
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
            "smv.jdbc.url=" + cls.url(),
            "smv.jdbc.driver=" + cls.driver()
        ]

    def test_SmvJdbcTable(self):
        df = self.createDF("K:String", "xxx")
        df.write.jdbc(self.url(), "MyJdbcTable", properties={"driver": "org.apache.derby.jdbc.EmbeddedDriver"})
        res = self.df("stage.modules.MyJdbcTable")
        res2 = self.df("stage.modules.MyJdbcWithQuery")
        self.should_be_same(res, df)
        self.should_be_same(res2, df)

    def test_publish_to_jdbc(self):
        fqn = "stage.modules.MyJdbcModule"
        m = self.load(fqn)[0]
        res = self.df(fqn)
        SmvModuleRunner([m], self.smvApp).publish_to_jdbc()
        readback = self.smvApp.sqlContext.read\
            .format("jdbc")\
            .option("url", self.url())\
            .option("dbtable", "MyJdbcModule")\
            .load()

        self.should_be_same(res, readback)


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
            "smv.conn.myjdbc_conn.class=smv.conn.SmvJdbcConnectionInfo",
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
    