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

import org.apache.spark.sql._
import org.joda.time._, format._
import scala.util.Try

/**
 * These methods were originally implemented in SmvDataSet.  They are
 * extracted to this utility class so they can be shared with
 * SmvModules written in Python.
 */
object SmvUtil {
  /**
   * Try to read a dataframe from a persisted file path, that is
   * usually an input data set or the output of an upstream SmvModule.
   *
   * The default format is headerless CSV with '"' as the quote
   * character
   */
  def readPersistedFile(sqlContext: SQLContext, path: String,
    attr: CsvAttributes = CsvAttributes.defaultCsv): Try[DataFrame] =
    Try {
      new FileIOHandler(sqlContext, path).csvFileWithSchema(attr)
    }

  /**
   * Save the dataframe content to disk, optionally generate edd.
   */
  def persist(sqlContext: SQLContext, dataframe: DataFrame, path: String, generateEdd: Boolean): Unit = {
    val fmt = DateTimeFormat.forPattern("HH:mm:ss")

    val counter = sqlContext.sparkContext.accumulator(0l)
    val before = DateTime.now()
    println(s"${fmt.print(before)} PERSISTING: ${path}")

    val df = dataframe.smvPipeCount(counter)
    val handler = new FileIOHandler(sqlContext, path)

    //Always persist null string as a special value with assumption that it's not
    //a valid data value
    handler.saveAsCsvWithSchema(df, strNullValue = "_SmvStrNull_")

    val after = DateTime.now()
    val runTime = PeriodFormat.getDefault().print(new Period(before, after))
    val n = counter.value

    println(s"${fmt.print(after)} RunTime: ${runTime}, N: ${n}")

    // if EDD flag was specified, generate EDD for the just saved file!
    // Use the "cached" file that was just saved rather than cause an action
    // on the input RDD which may cause some expensive computation to re-occur.
    if (generateEdd)
      readPersistedFile(sqlContext, path).get.edd.persistBesideData(path)
  }

  def publish(sqlContext: SQLContext, dataframe: DataFrame, path: String, generateEdd: Boolean): Unit = {
    val handler = new FileIOHandler(sqlContext, path)
    //Same as in persist, publish null string as a special value with assumption that it's not
    //a valid data value
    handler.saveAsCsvWithSchema(dataframe, strNullValue = "_SmvStrNull_")

    /* publish should also calculate edd if generarte Edd flag was turned on */
    if (generateEdd)
      dataframe.edd.persistBesideData(path)
  }

  /**
   * Exports a dataframe to a hive table.
   */
  def exportHive(sqlContext: SQLContext, dataframe: DataFrame, tableName: String): Unit = {
    // register the dataframe as a temp table.  Will be overwritten on next register.
    dataframe.registerTempTable("etable")
    sqlContext.sql(s"drop table if exists ${tableName}")
    sqlContext.sql(s"create table ${tableName} as select * from etable")
  }

}
