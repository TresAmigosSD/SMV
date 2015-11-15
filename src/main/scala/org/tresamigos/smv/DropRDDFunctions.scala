/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
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

//TODO: This file should be deleted once the DropRDDFunctions make it to the release.
//      This file is copied from : https://github.com/erikerlandson/spark/blob/469fbf6758959a793e2b21865ff214c9301b02fd/core/src/main/scala/org/apache/spark/rdd/DropRDDFunctions.scala#
//      Related article: http://erikerlandson.github.io/blog/2014/07/27/some-implications-of-supporting-the-scala-drop-method-for-spark-rdds/
//TODO: This file need to move to another directory

//package org.apache.spark.rdd
package org.tresamigos.smv

import scala.reflect.ClassTag

import org.apache.spark.{Logging, Partition, TaskContext}
import org.apache.spark.rdd._

/**
 * Extra functions available on RDDs for providing the RDD analogs of Scala drop,
 * dropRight and dropWhile, which return an RDD as a result
 */
private[smv] class DropRDDFunctions[T : ClassTag](self: RDD[T]) extends Logging with Serializable {

  /**
   * Return a new RDD formed by dropping the first (n) elements of the input RDD
   */
  def drop(n: Int):RDD[T] = {
    if (n <= 0) return self

    // locate partition that includes the nth element
    var rem = n
    var p = 0
    var np = 0
    while (rem > 0  &&  p < self.partitions.length) {
      val res = self.sparkContext.runJob(self, (it: Iterator[T]) => it.length,
        p until 1 + p)
      np = res(0)
      rem -= np
      p += 1
    }

    // all elements were dropped
    if (rem > 0  ||  (rem == 0  &&  p >= self.partitions.length)) {
      return self.sparkContext.emptyRDD[T]
    }

    // Return an RDD that discounts the first (n) elements of the parent RDD
    // (if we get here, note that rem <= 0)
    val pFirst = p - 1
    val pDrop = np + rem
    new RDD[T](self) {
      override def getPartitions: Array[Partition] = firstParent[T].partitions
      override val partitioner = self.partitioner
      override def compute(split: Partition, context: TaskContext):Iterator[T] = {
        if (split.index > pFirst) return firstParent[T].iterator(split, context)
        if (split.index == pFirst) return firstParent[T].iterator(split, context).drop(pDrop)
        Iterator.empty
      }
    }
  }


  /**
   * Return a new RDD formed by dropping the last (n) elements of the input RDD
   */
  def dropRight(n: Int):RDD[T] = {
    if (n <= 0) return self

    // locate partition that includes the nth element
    var rem = n
    var p = self.partitions.length-1
    var np = 0
    while (rem > 0  &&  p >= 0) {
      val res = self.sparkContext.runJob(self, (it: Iterator[T]) => it.length,
        p until 1 + p)
      np = res(0)
      rem -= np
      p -= 1
    }

    // all elements were dropped
    if (rem > 0  ||  (rem == 0  &&  p < 0)) {
      return self.sparkContext.emptyRDD[T]
    }

    // Return an RDD that discounts the last (n) elements of the parent RDD
    // (if we get here, note that rem <= 0)
    val pFirst = p + 1
    val pTake = -rem
    new RDD[T](self) {
      override def getPartitions: Array[Partition] = firstParent[T].partitions
      override val partitioner = self.partitioner
      override def compute(split: Partition, context: TaskContext):Iterator[T] = {
        if (split.index < pFirst) return firstParent[T].iterator(split, context)
        if (split.index == pFirst) return firstParent[T].iterator(split, context).take(pTake)
        Iterator.empty
      }
    }
  }


  /**
   * Return a new RDD formed by dropping leading elements until predicate function (f) returns false
   */
  def dropWhile(f: T=>Boolean):RDD[T] = {
    var p = 0
    var np = 0
    while (np <= 0  &&  p < self.partitions.length) {
      val res = self.sparkContext.runJob(self, (it: Iterator[T]) => it.dropWhile(f).length,
        p until 1 + p)
      np = res(0)
      p += 1
    }

    // all elements were dropped
    if (np <= 0  &&  p >= self.partitions.length) {
      return self.sparkContext.emptyRDD[T]
    }

    val pFirst = p - 1
    new RDD[T](self) {
      override def getPartitions: Array[Partition] = firstParent[T].partitions
      override val partitioner = self.partitioner
      override def compute(split: Partition, context: TaskContext):Iterator[T] = {
        if (split.index > pFirst) return firstParent[T].iterator(split, context)
        if (split.index == pFirst) return firstParent[T].iterator(split, context).dropWhile(f)
        Iterator.empty
      }
    }
  }

}
