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
import org.apache.spark.sql.{SchemaRDD, SQLContext}
import org.apache.spark.sql.execution.{SparkLogicalPlan, ExistingRdd}
import org.apache.spark.sql.catalyst.expressions.{GenericRow, Row}
import org.tresamigos.smv.StringConversionUtil._

class SqlContextHelper(sqlContext: SQLContext) {

  /** Create a SchemaRDD from RDD[Row] by applying a schema */
  def applySchemaToRowRDD(rdd: RDD[Row], schema: Schema): SchemaRDD = {
    sqlContext.applySchema(rdd, schema.toStructType)
  }

  /**
   * Create a SchemaRDD from RDD[Seq[Any]] by applying a schema.
   * It's the caller's responsibility to make sure the schema matches the data.
   */
  def applySchemaToSeqAnyRDD(rdd: RDD[Seq[Any]], schema: Schema): SchemaRDD = {
    applySchemaToRowRDD(rdd.map(r => new GenericRow(r.toArray)), schema)
  }

  /**
   * Drop the first rows that are considered as header if hasHeader flag is true.
   */
  private def dropHeader(strRDD: RDD[String], ca: CsvAttributes) : RDD[String] = {
    val noHeadRDD = if (ca.hasHeader) {
      // drop the first headerSize rows
      // TODO: The assumption that the header will be contained in the first partition is wrong
      strRDD.mapPartitionsWithIndex((idx: Int, rows: Iterator[String]) => {

        if (idx == 0) {
          rows.drop(ca.headerSize)
        }

        rows
      })
    } else {
      strRDD
    }

    noHeadRDD
  }

  /**
   * Create a SchemaRDD from a file by applying a Schema object.
   */
  def csvFileAddSchema(dataPath: String, schema: Schema)
                      (implicit ca: CsvAttributes, rejects: RejectLogger): SchemaRDD = {
    val strRDD = sqlContext.sparkContext.textFile(dataPath)
    val noHeadRDD = dropHeader(strRDD, ca)
    val rowRDD = noHeadRDD.csvToSeqStringRDD.seqStringRDDToRowRDD(schema)(rejects)
    applySchemaToRowRDD(rowRDD, schema)
  }

  /** Create an SchemaRDD from a data file and an schema file
   *
   *  @param dataPath CSV file location
   *  @param schemaPath the Schema file
   *  @param rejects the global reject logger, could be override by implicit val
   */
  def csvFileWithSchema(dataPath: String, schemaPath: String = null)
                       (implicit ca: CsvAttributes, rejects: RejectLogger): SchemaRDD = {
    val sp = if (schemaPath==null) Schema.dataPathToSchemaPath(dataPath) else schemaPath
    val sc = sqlContext.sparkContext
    val schema = Schema.fromFile(sc, sp)
    csvFileAddSchema(dataPath, schema)
  }

  /**
   * Extract the column names from the csv header if it has one. In case of multi-line header the last header line is
   * considered the one that holds the column names. If there is no header the columns will be named f1, f2, f3 ...
   * @param strRDD  holds the content of the csv file including the header if it has one.
   * @param ca the csv file attributes
   * @return the discovered schema
   */
  private def getColumnNames(strRDD: RDD[String], ca: CsvAttributes) : Array[String] = {
    val parser = new CSVParser(ca.delimiter)

    if (ca.hasHeader) {
      val completeHeader = strRDD.take(ca.headerSize)
      // Assuming the column names are going to be the last line of the header.
      // Is this the wrong assumption? Maybe
      val columnNamesRowStr = completeHeader.last
      var columnNames = parser.parseLine(columnNamesRowStr)
      //Trim end spaces and replace whitespaces with _
      columnNames = columnNames.map(_.trim).map(_.replaceAll("\\s+", "_"))

      columnNames
    } else {
      val firstRowStr = strRDD.first()
      val firstRowValues = parser.parseLine(firstRowStr)
      val numberOfColumns = firstRowValues.length

      val columnNames = for (i <- 1 to numberOfColumns)  yield "f" + i

      columnNames.toArray
    }
  }

  /**
   * Discover the time of a given column based on it value. Also perform type promotion to
   * accommodate all the possible values.
   */
  private def getSchemaEntry(curSchemaEntry: SchemaEntry, colName: String, valueStr: String) : SchemaEntry =  {
    if (valueStr.isEmpty)
      return curSchemaEntry

    curSchemaEntry match {
      //TODO: Still need to handle the case where a timestamp column get discovered as int when the date format is yyyyMMdd
      //      Also I do not want to treat any 8 digits id as a date.

      //Handling the initial case where the current column schema entry is not set yet
      case null if canConvertToInt(valueStr) => IntegerSchemaEntry(colName)
      case null if canConvertToLong(valueStr) => LongSchemaEntry(colName)
      case null if canConvertToFloat(valueStr) => FloatSchemaEntry(colName)
      case null if canConvertToDouble(valueStr) => DoubleSchemaEntry(colName)
      case null if canConvertToBoolean(valueStr) => BooleanSchemaEntry(colName)
      case null if canConvertToDate(valueStr,"yyyyMMdd") => TimestampSchemaEntry(colName,"yyyyMMdd")
      case null if canConvertToDate(valueStr,"dd/MM/yyyy") => TimestampSchemaEntry(colName,"dd/MM/yyyy")
      case null if canConvertToDate(valueStr,"dd-MM-yyyy") => TimestampSchemaEntry(colName,"dd-MM-yyyy")
      case null if canConvertToDate(valueStr,"dd-MMM-yyyy") => TimestampSchemaEntry(colName,"dd-MMM-yyyy")
      case null => StringSchemaEntry(colName)

      // Handling Integer type and its possible promotions
      case IntegerSchemaEntry( _ ) if canConvertToInt(valueStr) => curSchemaEntry
      case IntegerSchemaEntry( _ ) if canConvertToLong(valueStr) => LongSchemaEntry(colName)
      case IntegerSchemaEntry( _ ) if canConvertToFloat(valueStr) => FloatSchemaEntry(colName)
      case IntegerSchemaEntry( _ ) if canConvertToDouble(valueStr) => DoubleSchemaEntry(colName)
      case IntegerSchemaEntry( _ ) => StringSchemaEntry(colName)

      // Handling Long type and its possible promotions
      case LongSchemaEntry( _ ) if canConvertToLong(valueStr) => curSchemaEntry
      case LongSchemaEntry( _ ) if canConvertToDouble(valueStr) => DoubleSchemaEntry(colName)
      case LongSchemaEntry( _ ) => StringSchemaEntry(colName)

      // Handling Float type and its possible promotions
      case FloatSchemaEntry( _ ) if canConvertToFloat(valueStr) => curSchemaEntry
      case FloatSchemaEntry( _ ) if canConvertToDouble(valueStr) => DoubleSchemaEntry(colName)
      case FloatSchemaEntry( _ ) => StringSchemaEntry(colName)

      // Handling Double type and its possible promotions
      case DoubleSchemaEntry( _ ) if canConvertToDouble(valueStr) => curSchemaEntry
      case DoubleSchemaEntry( _ ) => StringSchemaEntry(colName)

      // Handling Boolean type and its possible promotions
      case BooleanSchemaEntry( _ ) if canConvertToBoolean(valueStr) => curSchemaEntry
      case BooleanSchemaEntry( _ ) => StringSchemaEntry(colName)

      //The date format should not change, if it did then we will treat the column as String
      case TimestampSchemaEntry(colName,"yyyyMMdd") if canConvertToDate(valueStr,"yyyyMMdd") =>  curSchemaEntry
      case TimestampSchemaEntry(colName,"yyyyMMdd") => StringSchemaEntry(colName)


      case TimestampSchemaEntry(colName,"dd/MM/yyyy") if canConvertToDate(valueStr,"dd/MM/yyyy") => curSchemaEntry
      case TimestampSchemaEntry(colName,"dd/MM/yyyy") => StringSchemaEntry(colName)

      case TimestampSchemaEntry(colName,"dd-MM-yyyy") if canConvertToDate(valueStr,"dd-MM-yyyy") => curSchemaEntry
      case TimestampSchemaEntry(colName,"dd-MM-yyyy") => StringSchemaEntry(colName)

      case TimestampSchemaEntry(colName,"dd-MMM-yyyy") if canConvertToDate(valueStr,"dd-MMM-yyyy") => curSchemaEntry
      case TimestampSchemaEntry(colName,"dd-MMM-yyyy") => StringSchemaEntry(colName)

      case StringSchemaEntry( _ ) => curSchemaEntry

      case _ => StringSchemaEntry(colName)
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
  private[smv] def discoverSchema(strRDD: RDD[String], numLines: Int, ca: CsvAttributes) : Schema = {
    val parser = new CSVParser(ca.delimiter)

    val columns = getColumnNames(strRDD,ca)

    val noHeadRDD = dropHeader(strRDD, ca)

    var schemaEntries = new scala.collection.mutable.ArrayBuffer[SchemaEntry]
    for (i <- 0 until columns.length) schemaEntries += null

    //TODO: What if the numLines is so big that rowsToParse will not fit in memory
    // An alternative is to use the mapPartitionWithIndex
    val rowsToParse = noHeadRDD.take(numLines)

    val columnsWithIndex = columns.zipWithIndex

    for (rowStr <- rowsToParse) {
      val rowValues = parser.parseLine(rowStr)
      if (rowValues.length == columnsWithIndex.length ) {
        for ((colName, index) <- columnsWithIndex) {
          val colVal = rowValues(index)
          if (colVal.nonEmpty) {
            schemaEntries(index) = getSchemaEntry(schemaEntries(index), colName, colVal )
          }
        }
      }
    }

    //Now we should set the null schema entries to the Default StringSchemaEntry. This should be the case when the first
    //numLines values for a given column happen to be missing.
    for ((colName, index) <- columnsWithIndex) {
      if (schemaEntries(index) == null) {
        schemaEntries(index) = StringSchemaEntry(colName)
      }
    }

    new Schema(schemaEntries.toSeq)
  }

  /**
   * Create a SchemaRDD from a file after discovering its schema
   * @param dataPath the path to the csv file
   * @param numLines the number of rows to process in order to discover the column types
   * @param ca the csv file attributes
   */
  def csvFileWithSchemaDiscovery(dataPath: String, numLines: Int = 1000)
                                (implicit ca: CsvAttributes, rejects: RejectLogger): SchemaRDD =  {
    val strRDD = sqlContext.sparkContext.textFile(dataPath)
    val schema = discoverSchema(strRDD, numLines, ca)
    val noHeadRDD = dropHeader(strRDD, ca)
    val rowRDD = noHeadRDD.csvToSeqStringRDD.seqStringRDDToRowRDD(schema)(rejects)
    applySchemaToRowRDD(rowRDD, schema)
  }
}
