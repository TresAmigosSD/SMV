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
import scala.reflect.ClassTag

/**
 * A wrapper class of the opencsv.CSVParser
 *
 *  Takes a function as a parameter to apply the function on parsed result on
 *  each record.
 *
 *  @param delimiter of the CSV record
 *  @param f the function applied to the parsed record
 */
class CSVStringParser[U](delimiter: Char, f: (String, Seq[String]) => U)(implicit ut: ClassTag[U]) extends Serializable {
  //import au.com.bytecode.opencsv.CSVParser

  /** Parse an Iterator[String], apply function "f", and return another Iterator */
  def parseCSV(iterator: Iterator[String])(rejects: RejectLogger): Iterator[U] = {
    val parser = new CSVParser(delimiter)
    iterator.map { r =>
      try {
        val parsed = parser.parseLine(r)
        Some(f(r,parsed))
      } catch {
        case e:java.io.IOException  =>  rejects.addRejectedLineWithReason(r,e); None
        case e:IndexOutOfBoundsException  =>  rejects.addRejectedLineWithReason(r,e); None
      }
    }.collect{case Some(l) => l}
  }
}

class CsvRDDHelper(rdd: RDD[String]) {

  /** Parse an RDD[String] to RDD[Seq[String]] */
  def csvToSeqStringRDD(delimiter: Char = ',')(implicit rejects: RejectLogger): RDD[Seq[String]] = {
    val parser = new CSVStringParser[Seq[String]](delimiter, (r:String, parsed:Seq[String]) => parsed)
    rdd.mapPartitions{ parser.parseCSV(_)(rejects) }
  }

  /**
   * Add an Index Key to each record.
   *
   *
   *  For an RDD[String], generate an RDD[(String, String)], where the first
   *  "String" of each tuple is a key field specified by the parameters, and
   *  the second "String" is the original record line
   *
   *  @param index the index of the field(s) in the record which should be
   *               considered as the Key(s)
   *  @param delimiter of the CSV records
   */
  def csvAddKey(index: Int*)(delimiter: Char = ',')(implicit rejects: RejectLogger): RDD[(String,String)] = {
    /* TODO: this is doing too much at once.  it really should be split in two
     * distinct steps that can be chained.  The addKey should be done using
     * symbol after a schema is applied rather than index (too fragile).
     */
    val i = if (index.isEmpty) Seq(0) else index
    val parser = new CSVStringParser[(String, String)](delimiter,
      (r:String, parsed:Seq[String]) => {
        val key = i.map(parsed(_)).mkString("")
        (key,r)
      }
    )
    rdd.mapPartitions{ parser.parseCSV(_)(rejects) }
  }
}

case class CsvAttributes(
  val delimiter: Char = ',',
  val quotechar: Char = '\"')

object CsvAttributes {
  implicit val defaultCsvAttrib = new CsvAttributes();
}

