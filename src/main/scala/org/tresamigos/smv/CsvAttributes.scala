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

case class CsvAttributes(
                          val delimiter: Char = ',',
                          val quotechar: Char = '\"',
                          val hasHeader: Boolean = false) {
  def isExcelCSV: Boolean = (quotechar == '"')
}

object CsvAttributes {
  implicit val defaultCsv = new CsvAttributes()

  // common CsvAttributes combos to be imported explicitly
  val defaultTsv = new CsvAttributes(delimiter = '\t')
  val defaultCsvWithHeader = new CsvAttributes(hasHeader = true)
  val defaultTsvWithHeader = new CsvAttributes(delimiter = '\t', hasHeader = true)

  def dropHeader[T](rdd: RDD[T])(implicit tt: ClassTag[T]) = {
    val dropFunc = new DropRDDFunctions(rdd)
    dropFunc.drop(1)
  }
}
