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
import org.apache.spark.SparkContext._

class SingleStrRDDHelper(rdd: RDD[String]) {
  //import au.com.bytecode.opencsv.CSVParser

  // TODO: csvToFields is a duplicate of what is done at the schema rdd helper.  Need to extract the common part!!!
  // Copy-n-paste is EVIL!!!! (DRY)
  def csvToFields(delimiter: Char = ',') = {
    rdd.mapPartitions { iterator =>
      val parser = new CSVParser(delimiter)
      iterator.map { l => parser.parseLine(l) }
    }
  }

  // TODO: we need a separate out the csv parsing from the key creation.
  // There is too much going on in here.  Need to create a csv parser class that converts a RDD[String]
  // to RDD[Seq[Any]] or just RDD[Row].  Then the addKey can be done on any RDD[Row]
  def csvAddKey(index: Int = 0, delimiter: Char = ',') = {
    rdd.mapPartitions { iterator =>
      val parser = new CSVParser(delimiter)
      iterator.map { l => (parser.parseLine(l)(index),l) }
    }
  }

  // TODO: this needs to be added to a generic RDD helper (RDD[Any]).  right now it can only be used on RDD[String]
  def saveAsGZFile(outFile: String) {
    rdd.saveAsTextFile(outFile,classOf[org.apache.hadoop.io.compress.GzipCodec])
  }

}

class KeyStrRDDHelper(rdd: RDD[(String, String)]) {
  // TODO: what is the use case of this?  why restrict it to RDD[String, String] as opposed to any PairRDD == RDD[(K,V)]
  def hashPartition( p: Int ) = {
    val partObj = new org.apache.spark.HashPartitioner(p)
    rddToPairRDDFunctions(rdd).partitionBy(partObj).values
  }

  def hashSample( fraction: Double, seed: Int = 141073) = {
    import scala.util.hashing.{MurmurHash3=>MH3}
    val key_rdd = rdd.filter{l=>
      (MH3.stringHash(l._1,seed)&0x7FFFFFFF) < (fraction*0x7FFFFFFF)
    }
    rddToPairRDDFunctions(key_rdd).values
  }

}
