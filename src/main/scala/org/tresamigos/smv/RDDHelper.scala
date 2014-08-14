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
import org.apache.spark.sql.catalyst.expressions.{GenericMutableRow, GenericRow, Row}
import org.apache.spark.SparkContext._
import scala.reflect.ClassTag


class CsvRDDHelper(rdd: RDD[String]) {

  def csvToSeqStringRDD(delimiter: Char): RDD[Seq[String]] = {
    //import au.com.bytecode.opencsv.CSVParser
    rdd.mapPartitions { iterator =>
      val parser = new CSVParser(delimiter)
      iterator.map { r =>
        parser.parseLine(r)
      }
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

}

abstract class SeqRDDHelper {
  protected def rowRDDToSchemaRDD(sqlContext: SQLContext, data: RDD[Row], schema: Schema): SchemaRDD = {
    val eRDD = ExistingRdd(schema.toAttribSeq, data)
    new SchemaRDD(sqlContext, SparkLogicalPlan(eRDD))
  }
}

class SeqStringRDDHelper(rdd: RDD[Seq[String]]) extends SeqRDDHelper {
  private def seqStringRDDToRowRDD(data: RDD[Seq[String]], schema: Schema)(rejects: RejectLogger): RDD[Row] = {
    data.mapPartitions { iterator =>
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

  def seqStringRDDToSchemaRDD(sqlContext: SQLContext, schema: Schema)(implicit rejects: RejectLogger): SchemaRDD = {
    //match rdd to Seq[String]
    rowRDDToSchemaRDD(sqlContext, seqStringRDDToRowRDD(rdd, schema)(rejects), schema)
  }
}

class SeqAnyRDDHelper(rdd: RDD[Seq[Any]]) extends SeqRDDHelper {
  def seqAnyRDDToSchemaRDD(sqlContext: SQLContext, schema: Schema): SchemaRDD ={
    rowRDDToSchemaRDD(sqlContext, rdd.map(r => new GenericRow(r.toArray)), schema)
  }
}

// sc.textFile("datafile").csvToSeqStringRDD(',').seqStringRDDToSchemaRDD(sqlContext, schema)


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
