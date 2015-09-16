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

import scala.reflect.ClassTag
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.catalyst.expressions.{GenericMutableRow, Row}


/**
 * A class to convert Csv strings to DF
 **/
private[smv] class CsvStringHandler(sqlContext: SQLContext, parserValidator: ParserValidation) {

  private[smv] def seqStringRDDToDF(
    rdd: RDD[Seq[String]],
    schema: SmvSchema
  ) = {
    val parserV = parserValidator
    val add: (Exception, String) => Unit = {(e,r) => parserV.addWithReason(e,r)}
    val rowRdd = rdd.mapPartitions{ iterator =>
      val mutableRow = new GenericMutableRow(schema.getSize)
      iterator.map { r =>
        try {
          require(r.size == schema.getSize)
          for (i <- 0 until schema.getSize) {
            mutableRow.update(i, schema.toValue(i, r(i)))
          }
          Some(mutableRow)
        } catch {
          case e:IllegalArgumentException  => add(e, r.mkString(",")); None
          case e:NumberFormatException  => add(e, r.mkString(",")); None
          case e:java.text.ParseException  =>  add(e, r.mkString(",")); None
        }
      }.collect{case Some(l) => l.asInstanceOf[Row]}
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
    f: (String, Seq[String]) => U, parserV: ParserValidation
  )(implicit ut: ClassTag[U]) extends Serializable {
  //import au.com.bytecode.opencsv.CSVParser

  /** Parse an Iterator[String], apply function "f", and return another Iterator */
  def parseCSV(iterator: Iterator[String])
              (implicit ca: CsvAttributes): Iterator[U] = {
    val parser = new CSVParser(ca.delimiter)
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
