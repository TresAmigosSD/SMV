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

package org.tresamigos.smv.jdbc

import org.apache.spark.sql.types._
import org.apache.spark.sql.jdbc._
import java.sql.Types

private[smv] object JdbcDialectHelper {
  def registerDerby() = JdbcDialects.registerDialect(DerbyDialect)
}

/**
 * Tells spark how to create queries for Derby. This dialect is copied over from
 * Spark 2.1, as it is not available in 1.5. When we move to 2.1, we should
 * remove it - it will be registered by Spark as a dialect already.
 *
 * NOTE: This should be used for tests only!
 */
private[smv] object DerbyDialect extends JdbcDialect {

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
