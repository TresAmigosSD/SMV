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

import org.apache.spark.sql.types._
import org.apache.spark.sql.jdbc._
import java.sql.Types

class SmvJdbcTest extends SmvTestUtil {
  val url = s"jdbc:derby:${testcaseTempDir}/derby;create=true"

  override def appArgs =
    super.appArgs ++ Seq("--smv-props", s"smv.jdbc.url=${url}")

  override def beforeAll = {
    super.beforeAll
    // This can be removed when we move to Spark 2.1. A Derby dialect is already
    // registered in 2.1
    JdbcDialects.registerDialect(DerbyDialect)
  }

  test("Test module can publish through JDBC") {
    JdbcModules.Publishable.publishThroughJDBC
    val Mdf = JdbcModules.Publishable.rdd()
    val readDf =
      app.sqlContext.read
        .format("jdbc")
        .option("url", url)
        .option("dbtable", JdbcModules.Publishable.tableName)
        .load()
    assertDataFramesEqual(Mdf, readDf)
  }

  test("Test SmvJdbcTable can read table from JDBC") {
    val df = app.createDF("k:String", "")
    df.write.jdbc(url, JdbcModules.Readable.tableName, new java.util.Properties())
    val tableDF = JdbcModules.Readable.rdd()
    assertDataFramesEqual(tableDF, df)
  }
}

package JdbcModules {
  object Publishable extends SmvModule("Publishable") with SmvOutput {
    override def tableName = "Publishable"
    override def run(i: runParams) = app.createDF("k:String", "")
    override def requiresDS = Seq()
  }

  object Readable extends SmvJdbcTable("Readable")
}

/**
 * Tells spark how to create queries for Derby. This dialect is copied over from
 * Spark 2.1, as it is not available in 1.5. When we move to 2.1, we should
 * remove it - it will be registered by Spark as a dialect already.
 */
private object DerbyDialect extends JdbcDialect {

  override def canHandle(url: String): Boolean = url.startsWith("jdbc:derby")

  override def getCatalystType(
      sqlType: Int, typeName: String, size: Int, md: MetadataBuilder): Option[DataType] = {
    if (sqlType == Types.REAL) Option(FloatType) else None
  }

  override def getJDBCType(dt: DataType): Option[JdbcType] = dt match {
    case StringType => Option(JdbcType("CLOB", java.sql.Types.CLOB))
    case ByteType => Option(JdbcType("SMALLINT", java.sql.Types.SMALLINT))
    case ShortType => Option(JdbcType("SMALLINT", java.sql.Types.SMALLINT))
    case BooleanType => Option(JdbcType("BOOLEAN", java.sql.Types.BOOLEAN))
    // 31 is the maximum precision and 5 is the default scale for a Derby DECIMAL
    case t: DecimalType if t.precision > 31 =>
      Option(JdbcType("DECIMAL(31,5)", java.sql.Types.DECIMAL))
    case _ => None
  }
}
