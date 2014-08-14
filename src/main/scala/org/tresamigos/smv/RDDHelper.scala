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

import org.apache.spark.sql.{SchemaRDD, SQLContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.execution.{SparkLogicalPlan, ExistingRdd}
import org.apache.spark.sql.catalyst.expressions.{GenericMutableRow, Row}
import org.apache.spark.SparkContext._
import scala.reflect.ClassTag


class CSVStringParser[U](delimiter: Char, f: (String, Seq[String]) => U)(implicit ut: ClassTag[U]) extends Serializable {
  //import au.com.bytecode.opencsv.CSVParser
  def parseCSV(iterator: Iterator[String]): Iterator[U] ={
    val parser = new CSVParser(delimiter)
    iterator.map { r => 
      val parsed = parser.parseLine(r)
      f(r,parsed)
    }
  }
}

class CsvRDDHelper(rdd: RDD[String]) {

  def csvToSeqStringRDD(delimiter: Char = ','): RDD[Seq[String]] = {
    val parser = new CSVStringParser[Seq[String]](delimiter, (r:String, parsed:Seq[String]) => parsed)
    rdd.mapPartitions{ parser.parseCSV(_) }
  }

  def csvAddKey(index: Int = 0, delimiter: Char = ','): RDD[(String,String)] = {
    val parser = new CSVStringParser[(String, String)](delimiter, (r:String, parsed:Seq[String]) => (parsed(index), r))
    rdd.mapPartitions{ parser.parseCSV(_) }
  }
}

class SeqStringRDDHelper(rdd: RDD[Seq[String]]) {
  def seqStringRDDToRowRDD(schema: Schema)(rejects: RejectLogger): RDD[Row] = {
    rdd.mapPartitions { iterator =>
      val mutableRow = new GenericMutableRow(schema.getSize)
      iterator.map { r =>
        try {
          require(r.size == schema.getSize)
          for (i <- 0 until schema.getSize) {
            mutableRow.update(i, schema.toValue(i, r(i)))
          }
          Some(mutableRow)
        } catch {
          case e:IllegalArgumentException  =>  rejects.addRejectedSeqWithReason(r,e); None
          case e:NumberFormatException  =>  rejects.addRejectedSeqWithReason(r,e); None
          case e:java.text.ParseException  =>  rejects.addRejectedSeqWithReason(r,e); None
        }
      }.collect{case Some(l) => l}
    }
  }
}

class RDDHelper[T](rdd: RDD[T])(implicit tt: ClassTag[T]){
  def saveAsGZFile(outFile: String) {
    rdd.saveAsTextFile(outFile,classOf[org.apache.hadoop.io.compress.GzipCodec])
  }
}

class PairRDDHelper[K,V](rdd: RDD[(K, V)])(implicit kt: ClassTag[K], vt: ClassTag[V]) {
  def hashPartition( p: Int ) = {
    val partObj = new org.apache.spark.HashPartitioner(p)
    rddToPairRDDFunctions(rdd).partitionBy(partObj).values
  }

  def hashSample( fraction: Double, seed: Int = 141073) = {
    import scala.util.hashing.{MurmurHash3=>MH3}
    val key_rdd = rdd.filter{case (k,v) =>
      (MH3.stringHash(k.toString, seed)&0x7FFFFFFF) < (fraction*0x7FFFFFFF)
    }
    rddToPairRDDFunctions(key_rdd).values
  }
}
