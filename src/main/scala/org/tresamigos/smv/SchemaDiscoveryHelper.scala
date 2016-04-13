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

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SQLContext}
import org.tresamigos.smv.StringConversionUtil._

import scala.util.Try

class SchemaDiscoveryHelper(sqlContext: SQLContext) {
  /**
   * Extract the column names from the csv header if it has one. In case of multi-line header the last header line is
   * considered the one that holds the column names. If there is no header the columns will be named f1, f2, f3 ...
   * @param strRDD  holds the content of the csv file including the header if it has one.
   * @param ca the csv file attributes
   * @return the discovered schema
   */
  private def getColumnNames(strRDD: RDD[String], ca: CsvAttributes) : Array[String] = {
    val parser = new CSVParserWrapper(ca)

    if (ca.hasHeader) {
      val columnNamesRowStr = strRDD.first()
      var columnNames = parser.parseLine(columnNamesRowStr)
      columnNames = columnNames.map(_.trim).map(SchemaEntry.valueToColumnName(_))

      columnNames
    } else {
      val firstRowStr = strRDD.first()
      val firstRowValues = parser.parseLine(firstRowStr)
      val numberOfColumns = firstRowValues.length

      val columnNames = for (i <- 1 to numberOfColumns)  yield "f" + i

      columnNames.toArray
    }
  }

  private def canConvertToyyyyMMddDate(str: String) : Boolean = {
    if (str.length == 8) {
     val monthVal = str.substring(4,6).toInt
     if (monthVal < 1 || monthVal > 12) return false

     val dayVal = str.substring(6,8).toInt
     if (dayVal < 1 || dayVal > 31) return false

     true
    } else {
      false
    }
  }

  /**
   * Discover the type of a given column based on it value. Also perform type promotion to
   * accommodate all the possible values.
   * TODO: should consider using Decimal for large integer/float values (more than what can fit in long/double)
   */
  private def getTypeFormat(curTypeFormat: TypeFormat, valueStr: String) : TypeFormat =  {
    if (valueStr.isEmpty)
      return curTypeFormat

    curTypeFormat match {

      //Handling the initial case where the current column schema entry is not set yet
      case null if canConvertToInt(valueStr) && canConvertToyyyyMMddDate(valueStr)
                                             => TimestampTypeFormat("yyyyMMdd")
      case null if canConvertToInt(valueStr) => IntegerTypeFormat()
      case null if canConvertToLong(valueStr) => LongTypeFormat()
      case null if canConvertToFloat(valueStr) => FloatTypeFormat()
      case null if canConvertToDouble(valueStr) => DoubleTypeFormat()
      case null if canConvertToBoolean(valueStr) => BooleanTypeFormat()
      case null if canConvertToDate(valueStr,"dd/MM/yyyy") => TimestampTypeFormat("dd/MM/yyyy")
      case null if canConvertToDate(valueStr,"dd-MM-yyyy") => TimestampTypeFormat("dd-MM-yyyy")
      case null if canConvertToDate(valueStr,"dd-MMM-yyyy") => TimestampTypeFormat("dd-MMM-yyyy")
      case null if canConvertToDate(valueStr,"ddMMMyyyy") => TimestampTypeFormat("ddMMMyyyy")
      case null if canConvertToDate(valueStr,"yyyy-MM-dd") => TimestampTypeFormat("yyyy-MM-dd")
      case null => StringTypeFormat()

      // Handling Integer type and its possible promotions
      case IntegerTypeFormat( _ ) if canConvertToInt(valueStr) => curTypeFormat
      case IntegerTypeFormat( _ ) if canConvertToLong(valueStr) => LongTypeFormat()
      case IntegerTypeFormat( _ ) if canConvertToFloat(valueStr) => FloatTypeFormat()
      case IntegerTypeFormat( _ ) if canConvertToDouble(valueStr) => DoubleTypeFormat()
      case IntegerTypeFormat( _ ) => StringTypeFormat()

      // Handling Long type and its possible promotions
      case LongTypeFormat( _ ) if canConvertToLong(valueStr) => curTypeFormat
      case LongTypeFormat( _ ) if canConvertToDouble(valueStr) => DoubleTypeFormat()
      case LongTypeFormat( _ ) => StringTypeFormat()

      // Handling Float type and its possible promotions
      case FloatTypeFormat( _ ) if canConvertToFloat(valueStr) => curTypeFormat
      case FloatTypeFormat( _ ) if canConvertToDouble(valueStr) => DoubleTypeFormat()
      case FloatTypeFormat( _ ) => StringTypeFormat()

      // Handling Double type and its possible promotions
      case DoubleTypeFormat( _ ) if canConvertToDouble(valueStr) => curTypeFormat
      case DoubleTypeFormat( _ ) => StringTypeFormat()

      // Handling Boolean type and its possible promotions
      case BooleanTypeFormat( _ ) if canConvertToBoolean(valueStr) => curTypeFormat
      case BooleanTypeFormat( _ ) => StringTypeFormat()

      //TODO: Need to find a better way to match dates to avoid repetition.

      //The date format should not change, if it did then we will treat the column as String
      case TimestampTypeFormat("yyyyMMdd") if canConvertToyyyyMMddDate(valueStr) =>  curTypeFormat
      case TimestampTypeFormat("yyyyMMdd") if canConvertToInt(valueStr) => IntegerTypeFormat()
      case TimestampTypeFormat("yyyyMMdd") if canConvertToLong(valueStr) => LongTypeFormat()
      case TimestampTypeFormat("yyyyMMdd") if canConvertToFloat(valueStr) => FloatTypeFormat()
      case TimestampTypeFormat("yyyyMMdd") if canConvertToDouble(valueStr) => DoubleTypeFormat()
      case TimestampTypeFormat("yyyyMMdd") => StringTypeFormat()

      case TimestampTypeFormat("dd/MM/yyyy") if canConvertToDate(valueStr,"dd/MM/yyyy") => curTypeFormat
      case TimestampTypeFormat("dd/MM/yyyy") => StringTypeFormat()

      case TimestampTypeFormat("dd-MM-yyyy") if canConvertToDate(valueStr,"dd-MM-yyyy") => curTypeFormat
      case TimestampTypeFormat("dd-MM-yyyy") => StringTypeFormat()

      case TimestampTypeFormat("dd-MMM-yyyy") if canConvertToDate(valueStr,"dd-MMM-yyyy") => curTypeFormat
      case TimestampTypeFormat("dd-MMM-yyyy") => StringTypeFormat()

      case TimestampTypeFormat("ddMMMyyyy") if canConvertToDate(valueStr,"ddMMMyyyy") => curTypeFormat
      case TimestampTypeFormat("ddMMMyyyy") => StringTypeFormat()

      case TimestampTypeFormat("yyyy-MM-dd") if canConvertToDate(valueStr,"yyyy-MM-dd") => curTypeFormat
      case TimestampTypeFormat("yyyy-MM-dd") => StringTypeFormat()

      case StringTypeFormat( _ , _ ) => curTypeFormat

      case _ => StringTypeFormat()
    }
  }

  /**
   * Discover the schema associated with a csv file that was converted to RDD[String]. If the csv file have a header,
   * the column names will be what the header specify, in case of multi-line header, the last line in the header is
   * considered the one that specify the column names. If there is no header the column names will be f1, f2, ... fn.
   * @param strRDD the content of the csv file read as RDD[String]. This should include the header if the csv file has one.
   * @param numLines the number of rows to process to discover the type of the columns
   * @param ca  the csv file attributes
   */
  private[smv] def discoverSchema(strRDD: RDD[String], numLines: Int, ca: CsvAttributes) : SmvSchema = {
    val parser = new CSVParser(ca.delimiter)

    val columns = getColumnNames(strRDD,ca)

    val noHeadRDD = if (ca.hasHeader) CsvAttributes.dropHeader(strRDD) else strRDD

    var typeFmts = new scala.collection.mutable.ArrayBuffer[TypeFormat]
    for (i <- 0 until columns.length) typeFmts += null

    //TODO: What if the numLines is so big that rowsToParse will not fit in memory
    // An alternative is to use the mapPartitionWithIndex
    val rowsToParse = noHeadRDD.take(numLines)

    val columnsWithIndex = columns.zipWithIndex

    var validCount = 0
    for (rowStr <- rowsToParse) {
      val rowValues = Try{parser.parseLine(rowStr)}.getOrElse(Array[String]())
      if (rowValues.length == columnsWithIndex.length ) {
        validCount += 1
        for (index <- 0 until columns.length) {
          val colVal = rowValues(index)
          if (colVal.nonEmpty) {
            typeFmts(index) = getTypeFormat(typeFmts(index), colVal )
          }
        }
      }
    }

    // handle case where we were not able to parse a single valid data line.
    if (validCount == 0) {
      throw new IllegalStateException("Unable to find a single valid data line")
    }

    //Now we should set the null schema entries to the Default StringSchemaEntry. This should be the case when the first
    //numLines values for a given column happen to be missing.
    for (index <- 0 until columns.length) {
      if (typeFmts(index) == null) {
        typeFmts(index) = StringTypeFormat()
      }
    }

    new SmvSchema(columns.zip(typeFmts).map{case (n, t) => SchemaEntry(n, t)}, Map.empty)
  }

  /**
   * Discover schema from file
   * @param dataPath the path to the csv file (a schema file should be a sister file of the csv)
   * @param numLines the number of rows to process in order to discover the column types
   * @param ca the csv file attributes
   */
  def discoverSchemaFromFile(dataPath: String, numLines: Int = 1000)
                            (implicit ca: CsvAttributes): SmvSchema = {
    val strRDD = sqlContext.sparkContext.textFile(dataPath)
    discoverSchema(strRDD, numLines, ca)
  }
}
