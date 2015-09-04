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

/**
 * A wrapper class of the opencsv.CSVParser
 *
 *  Takes a function as a parameter to apply the function on parsed result on
 *  each record.
 *
 *  @param f the function applied to the parsed record
 */
class CSVStringParser[U](f: (String, Seq[String]) => U)(implicit ut: ClassTag[U]) extends Serializable {
  //import au.com.bytecode.opencsv.CSVParser

  /** Parse an Iterator[String], apply function "f", and return another Iterator */
  def parseCSV(iterator: Iterator[String])
              (implicit ca: CsvAttributes, rejects: RejectLogger): Iterator[U] = {
    val parser = new CSVParser(ca.delimiter)
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

  def parseFrl(iterator: Iterator[String], startLenPairs: Seq[(Int, Int)])
              (implicit rejects: RejectLogger): Iterator[U] = {
    iterator.map{r =>
      try {
        val parsed =  startLenPairs.map{ case (start, len) =>
          r.substring(start, start + len)
        }
        Some(f(r,parsed))
      } catch {
        case e:Exception => rejects.addRejectedLineWithReason(r, e); None
      }
    }.collect{case Some(l) => l}
  }
}
