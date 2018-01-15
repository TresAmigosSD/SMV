/*
 * This file is licensed under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.tresamigos.smv

import org.apache.spark.sql.jdbc.JdbcDialects
import org.apache.spark.sql.functions.col

class SmvJdbcTest extends SmvTestUtil {
  val url = s"jdbc:derby:${testcaseTempDir}/derby;create=true"

  override def appArgs =
    super.appArgs ++ Seq("--smv-props", s"smv.jdbc.url=${url}")

  override def beforeAll = {
    super.beforeAll
    // This can be removed when we move to Spark 2.1. A Derby dialect is already
    // registered in 2.1
    JdbcDialects.registerDialect(jdbc.DerbyDialect)
  }

  test("Test module can publish through JDBC") {
    JdbcModules.PublishableMod.publishThroughJDBC(collector=new SmvRunInfoCollector)
    val publishableDf = JdbcModules.PublishableMod.rdd(collector=new SmvRunInfoCollector)
    val readDf =
      app.sqlContext.read
        .format("jdbc")
        .option("url", url)
        .option("dbtable", JdbcModules.PublishableMod.tableName)
        .load()
    assertDataFramesEqual(publishableDf, readDf)
  }

  test("Test SmvJdbcTable can read table from JDBC") {
    val df = app.createDF("k:String", "")
    df.write.jdbc(url, JdbcModules.ReadableMod.tableName, new java.util.Properties())
    val tableDF = JdbcModules.ReadableMod.rdd(collector=new SmvRunInfoCollector)
    assertDataFramesEqual(tableDF, df)
  }

  test("Test SmvJdbcTable can read table from JDBC with custom query") {
    val df = app.createDF("j:String;k:String", "abc,abc;abc,def;def,def")
    df.write.jdbc(url, JdbcModules.CustomQueryMod.tableName, new java.util.Properties())
    val tableDF = JdbcModules.CustomQueryMod.rdd(collector=new SmvRunInfoCollector)
    val expectedDF = df.where(col("k") === col("j"))
    assertDataFramesEqual(tableDF.orderBy(col("j").asc), expectedDF.orderBy(col("j").asc))
  }
}

package JdbcModules {
  object PublishableMod extends SmvModule("PublishableMod") with SmvOutput {
    override def tableName = "PublishableMod"
    override def run(i: runParams) = app.createDF("k:String", "")
    override def requiresDS = Seq()
  }

  object AppendMod extends SmvModule("AppendMod") with SmvOutput {
    override def tableName = "AppendMod"
    override def run(i: runParams) = app.createDF("k:String", "row1")
    override def requiresDS = Seq()
  }

  object CustomQueryMod extends SmvJdbcTable("CustomQueryMod") with SmvOutput {
    override def userQuery = "select * from CustomQueryMod where k like j"
  }

  object ReadableMod extends SmvJdbcTable("ReadableMod")
}
