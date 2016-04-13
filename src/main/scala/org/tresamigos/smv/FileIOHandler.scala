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

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.GenericRow
import scala.reflect.ClassTag
import org.apache.spark.rdd.RDD
import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.expressions.GenericMutableRow

import dqm.{ParserLogger, TerminateParserLogger}

/**
 * A class to convert Csv strings to DF
 **/
private[smv] class FileIOHandler(
    sqlContext: SQLContext,
    dataPath: String,
    schemaPath: Option[String] = None,
    parserValidator: ParserLogger = TerminateParserLogger
  ) {

  private def fullSchemaPath = schemaPath.getOrElse(SmvSchema.dataPathToSchemaPath(dataPath))

  /**
   * Create a DataFrame from the given data/schema path and CSV attributes.
   * If CSV attributes are null, then they are extracted from the schema file directly.
   */
  private[smv] def csvFileWithSchema(
    csvAttributes: CsvAttributes
  ): DataFrame = {
    val sc = sqlContext.sparkContext
    val schema = SmvSchema.fromFile(sc, fullSchemaPath)

    val ca = if (csvAttributes == null) schema.extractCsvAttributes() else csvAttributes

    val strRDD = sc.textFile(dataPath)
    val noHeadRDD = if (ca.hasHeader) CsvAttributes.dropHeader(strRDD) else strRDD

    csvStringRDDToDF(noHeadRDD, schema, ca)
  }

  private[smv] def frlFileWithSchema(): DataFrame = {
    val sc = sqlContext.sparkContext
    val slices = SmvSchema.slicesFromFile(sc, fullSchemaPath)
    val schema = SmvSchema.fromFile(sc, fullSchemaPath)
    require(slices.size == schema.getSize)

    // TODO: show we allow header in Frl files?
    val strRDD = sc.textFile(dataPath)
    frlStringRDDToDF(strRDD, schema, slices)
  }

  private def seqStringRDDToDF(
    rdd: RDD[Seq[String]],
    schema: SmvSchema
  ) = {
    val parserV = parserValidator
    val add: (Exception, Seq[_]) => Option[Row] = { (e,r) => parserV.addWithReason(e,r.mkString(",")); None }
    val rowRdd = rdd.mapPartitions{ iterator =>
      iterator.map[Option[Row]] { r =>
        try {
          require(r.size == schema.getSize)
          // use the schema to parse string into expected type
          val parsed = r.zipWithIndex map { case (elem, i) => schema.toValue(i, elem) }
          Some(Row.fromSeq(parsed))
        } catch {
          case e:IllegalArgumentException  => add(e, r);
          case e:NumberFormatException     => add(e, r);
          case e:java.text.ParseException  => add(e, r);
        }
      }.collect{case Some(l) => l}
    }
    sqlContext.createDataFrame(rowRdd, schema.toStructType)
  }

  private[smv] def csvStringRDDToDF(
    rdd: RDD[String],
    schema: SmvSchema,
    csvAttributes: CsvAttributes
  ) = {
    val parserV = parserValidator
    val parser = new CSVStringParser[Seq[String]]((r:String, parsed:Seq[String]) => parsed, parserV)
    val _ca = csvAttributes
    val seqStringRdd = rdd.mapPartitions{ parser.parseCSV(_)(_ca) }
    seqStringRDDToDF(seqStringRdd, schema)
  }

  private[smv] def frlStringRDDToDF(
    rdd: RDD[String],
    schema: SmvSchema,
    slices: Seq[Int]
  ) = {
    val parserV = parserValidator
    val startLen = slices.scanLeft(0){_ + _}.dropRight(1).zip(slices)
    val parser = new CSVStringParser[Seq[String]]((r:String, parsed:Seq[String]) => parsed, parserV)
    val seqStringRdd = rdd.mapPartitions{ parser.parseFrl(_, startLen) }
    seqStringRDDToDF(seqStringRdd, schema)
  }

  // TODO: add schema file path as well.
  // Since on Linux, when file stored on local file system, the partitions are not
  // guaranteed in order when read back in, we need to only store the body w/o the header
  private[smv] def saveAsCsvWithSchema(
      df: DataFrame,
      schemaWithMeta: SmvSchema = null,
      csvAttributes: CsvAttributes = CsvAttributes.defaultCsv,
      strNullValue: String = ""
    ) {

    val schema = if (schemaWithMeta == null) {SmvSchema.fromDataFrame(df, strNullValue)} else {schemaWithMeta}
    val schemaWithAttributes = schema.addCsvAttributes(csvAttributes)
    val qc = csvAttributes.quotechar

    //Adding the header to the saved file if ca.hasHeader is true.
    val fieldNames = df.schema.fieldNames
    val headerStr = fieldNames.map(_.trim).map(fn => qc + fn + qc).
      mkString(csvAttributes.delimiter.toString)

    val csvHeaderRDD = df.sqlContext.sparkContext.parallelize(Array(headerStr),1)
    val csvBodyRDD = df.map(schema.rowToCsvString(_, csvAttributes))

    //As far as I know the union maintain the order. So the header will end up being the
    //first line in the saved file.

    val csvRDD =
      if (csvAttributes.hasHeader) csvHeaderRDD.union(csvBodyRDD)
      else csvBodyRDD

    schemaWithAttributes.saveToFile(df.sqlContext.sparkContext, fullSchemaPath)
    csvRDD.saveAsTextFile(dataPath)
  }

}

/**
 * A non-generic wrapper class of opencsv.CSVParser.
 */
private[smv] class CSVParserWrapper(ca: CsvAttributes) {
  val parser = new CSVParser(ca.delimiter, ca.quotechar)

  // For Excel CSV formatted files unescape "" => " and suppress \ as an escape character.
  // For the other format, backslash is an escape character, so parse normally
  def parseLine(line: String) = {
    if (ca.isExcelCSV)
      parser.parseLine(line.replace("\\", "\\\\"))
    else
      parser.parseLine(line)
  }
}


/**
 * A wrapper class of the opencsv.CSVParser
 *
 *  Takes a function as a parameter to apply the function on parsed result on
 *  each record.
 *
 *  @param f the function applied to the parsed record
 */
private[smv] class CSVStringParser[U](
    f: (String, Seq[String]) => U, parserV: ParserLogger
  )(implicit ut: ClassTag[U]) extends Serializable {
  //import au.com.bytecode.opencsv.CSVParser

  /** Parse an Iterator[String], apply function "f", and return another Iterator */
  def parseCSV(iterator: Iterator[String])
              (implicit ca: CsvAttributes): Iterator[U] = {
    val parser = new CSVParserWrapper(ca)
    val add: (Exception, String) => Unit = {(e,r) => parserV.addWithReason(e,r)}
    iterator.map { r =>
      try {
        val parsed = parser.parseLine(r)
        Some(f(r,parsed))
      } catch {
        case e:java.io.IOException  =>  add(e, r); None
        case e:IndexOutOfBoundsException  =>  add(e, r); None
      }
    }.collect{case Some(l) => l}
  }

  def parseFrl(iterator: Iterator[String], startLenPairs: Seq[(Int, Int)]): Iterator[U] = {
    val add: (Exception, String) => Unit = {(e,r) => parserV.addWithReason(e,r)}
    iterator.map{r =>
      try {
        val parsed =  startLenPairs.map{ case (start, len) =>
          r.substring(start, start + len)
        }
        Some(f(r,parsed))
      } catch {
        case e:Exception => add(e,r); None
      }
    }.collect{case Some(l) => l}
  }
}
