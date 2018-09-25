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
        fqn = "stage.modules.MyJdbcCsvString"
        j_m = self.load(fqn)
        j_m.publishThroughJDBC(self.smvApp._jvm.SmvRunInfoCollector())
        res = self.df(fqn)
        readback = self.smvApp.sqlContext.read\
            .format("jdbc")\
            .option("url", self.url())\
            .option("dbtable", "MyJdbcOutput")\
            .load()

        self.should_be_same(res, readback)