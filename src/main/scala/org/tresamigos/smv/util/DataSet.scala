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
package util

import org.apache.spark.sql._
import org.joda.time._, format._

/**
 * These methods were originally implemented in SmvDataSet.  They are
 * extracted to this utility class so they can be shared with
 * SmvModules written in Python.
 */
object DataSet {

  /**
   * Read a dataframe from a persisted file path, that is usually an
   * input data set or the output of an upstream SmvModule.
   *
   * The default format is headerless CSV with '"' as the quote
   * character
   */
  def readFile(sqlContext: SQLContext,
               path: String,
               attr: CsvAttributes = CsvAttributes.defaultCsv): DataFrame =
    new FileIOHandler(sqlContext, path).csvFileWithSchema(attr)

  /**
   * Exports a dataframe to a hive table.
   */
  def exportDataFrameToHive(sqlContext: SQLContext,
                            dataframe: DataFrame,
                            tableName: String,
                            publishHiveSql: Option[String]): Unit = {
    // register the dataframe as a temp table.  Will be overwritten on next register.
    dataframe.registerTempTable("dftable")

    // if user provided a publish hive sql command, run it instead of default
    // table creation from data frame result.
    if (publishHiveSql.isDefined) {
      sqlContext.sql(publishHiveSql.get)
    } else {
      sqlContext.sql(s"drop table if exists ${tableName}")
      sqlContext.sql(s"create table ${tableName} as select * from dftable")
    }
  }
}
