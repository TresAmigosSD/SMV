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

/* SimpleApp.scala */
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf

import org.apache.spark.sql.SQLContext


// TODO: need to get rid of this entire example.  Use the example from MVD project!
object SimpleApp {

  def manOf[T: Manifest](t: T): Manifest[T] = manifest[T]

  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("Simple Application")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)
    import sqlContext._

    val srdd = sqlContext.csvFileWithSchema("data/in/test1_data.csv", "data/in/test1.schema")
    //srdd.saveAsCsvWithSchema("data/out/foo.csv") // this uses default csv attributes
    srdd.saveAsCsvWithSchema("data/out/foo.csv")(CsvAttributes(delimiter='|'))

    // TODO: read a previously written schema+data.

//    val res = srdd.select('val as 'val2).collect
//    val res = srdd.where('valx < 1.5).collect
    val res = srdd.where('val2 > 5.0).select('val).collect
    res.foreach(println(_))
  }
}
