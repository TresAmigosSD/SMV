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

package org.tresamigos

import org.apache.spark.sql.{SchemaRDD, SQLContext}
import org.apache.spark.rdd.RDD
import scala.language.implicitConversions
import scala.reflect.ClassTag

package object smv {
  implicit def makeSRHelper(sc: SQLContext) = new SqlContextHelper(sc)
  implicit def makeSDHelper(sc: SQLContext) = new SchemaDiscoveryHelper(sc)
  implicit def makeSchemaRDDHelper(srdd: SchemaRDD) = new SchemaRDDHelper(srdd)
  implicit def makePivotOp(srdd: SchemaRDD) = new PivotOp(srdd)

  implicit def makeCsvRDDHelper(rdd: RDD[String]) = new CsvRDDHelper(rdd)
  implicit def makeSeqStrRDDHelper(rdd: RDD[Seq[String]]) = new SeqStringRDDHelper(rdd)
  implicit def makeRDDHelper[T](rdd: RDD[T])(implicit tt: ClassTag[T]) = 
    new RDDHelper[T](rdd)
  implicit def makePairRDDHelper[K,V](rdd: RDD[(K, V)])(implicit kt: ClassTag[K], vt: ClassTag[V]) = 
    new PairRDDHelper[K,V](rdd)
}
