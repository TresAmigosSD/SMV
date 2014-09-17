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

import org.apache.spark.rdd.{DropRDDFunctions, RDD}
import org.apache.spark.sql.catalyst.expressions.{GenericMutableRow, Row}
import org.apache.spark.SparkContext._
import scala.reflect.ClassTag


class SeqStringRDDHelper(rdd: RDD[Seq[String]]) {

  /** Convert RDD[Seq[String]] to RDD[Row] based on given schema */
  def seqStringRDDToRowRDD(schema: Schema)(implicit rejects: RejectLogger): RDD[Row] = {
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
  /** Same as rdd.saveAsTextFile, but save as gzip file */
  def saveAsGZFile(outFile: String) {
    rdd.saveAsTextFile(outFile,classOf[org.apache.hadoop.io.compress.GzipCodec])
  }

  /**
   * Drop the specified number of first rows from the RDD and return the new one.
   */
  def dropRows( num: Int): RDD[T] = {
    if (num > 0) {
      val dropFunc = new DropRDDFunctions(rdd)
      dropFunc.drop(num)
    } else {
      rdd
    }

  }
}

class PairRDDHelper[K,V](rdd: RDD[(K, V)])(implicit kt: ClassTag[K], vt: ClassTag[V]) {
  /** Partition a RDD[(K,V)] according to a hash function on the Key
   *
   *  Partition based on the hash of a Key, so the record with the same key
   *  will always be on the same partition. 
   *
   *  @param p number of partitions
   *  @return RDD[V] the value part of the record only
   */
  def hashPartition( p: Int ) = {
    val partObj = new org.apache.spark.HashPartitioner(p)
    rddToPairRDDFunctions(rdd).partitionBy(partObj).values
  }

  /** Sample RDD[(K,V)] by a hash function on the Key
   * 
   *  Create a random sample from the input RDD according to a hash function
   *  on the Key.Guarantee records with the same key either all sampled in or 
   *  sampled out 
   * 
   *  @param fraction of sampling: 0.05 means 5% sample
   *  @param seed of the hash function 
   *  @return the sampel set RDD[V], the value part of the input only
   */
  def hashSample( fraction: Double, seed: Int = 141073) = {
    import scala.util.hashing.{MurmurHash3=>MH3}
    val key_rdd = rdd.filter{case (k,v) =>
      (MH3.stringHash(k.toString, seed)&0x7FFFFFFF) < (fraction*0x7FFFFFFF)
    }
    rddToPairRDDFunctions(key_rdd).values
  }
}
