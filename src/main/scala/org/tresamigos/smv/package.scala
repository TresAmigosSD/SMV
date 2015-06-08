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

import org.apache.spark.sql.functions._
import org.apache.spark.sql.{SchemaRDD, SQLContext}
import org.apache.spark.sql.{Column, ColumnName}
import org.apache.spark.sql.GroupedData
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.contrib.smv._
import org.apache.spark.rdd.RDD
import scala.language.implicitConversions
import scala.reflect.ClassTag
import scala.reflect.runtime.universe.TypeTag

package object smv {
  implicit def makeColHelper(col: Column) = new ColumnHelper(col)
  implicit def makeSymColHelper(sym: Symbol) = new ColumnHelper(new ColumnName(sym.name))
  implicit def makeSRHelper(sc: SQLContext) = new SqlContextHelper(sc)
  implicit def makeSDHelper(sc: SQLContext) = new SchemaDiscoveryHelper(sc)
  implicit def makeDFHelper(srdd: SchemaRDD) = new SmvDFHelper(srdd)
  implicit def makeSmvGDFunc(sgd: SmvGroupedData) = new SmvGroupedDataFunc(sgd)
  implicit def makeSmvGDCvrt(sgd: SmvGroupedData) = sgd.toGroupedData
  implicit def makeSmvCDSAggColumn(col: Column) = SmvCDSAggColumn(col.toExpr)
  implicit def makeCsvRDDHelper(rdd: RDD[String]) = new CsvRDDHelper(rdd)
  implicit def makeSeqStrRDDHelper(rdd: RDD[Seq[String]]) = new SeqStringRDDHelper(rdd)
  implicit def makeRDDHelper[T](rdd: RDD[T])(implicit tt: ClassTag[T]) = 
    new RDDHelper[T](rdd)
  implicit def makePairRDDHelper[K,V](rdd: RDD[(K, V)])(implicit kt: ClassTag[K], vt: ClassTag[V]) = 
    new PairRDDHelper[K,V](rdd)
    
  /***************************************************************************
   * Functions 
   ***************************************************************************/
  
  /* Aggregate Function wrappers */
  def histogram(c: Column) = {
    new Column(Histogram(c.toExpr))
  }
  
  def onlineAverage(c: Column) = {
    new Column(OnlineAverage(c.toExpr))
  }
  
  def onlineStdDev(c: Column) = {
    new Column(OnlineStdDev(c.toExpr))
  }
  
  /* NonAggregate Function warppers */
  def columnIf(cond: Column, l: Column, r: Column) = {
    new Column(If(cond.toExpr, l.toExpr, r.toExpr))
  }
  
  def smvStrCat(columns: Column*) = {
    new Column(SmvStrCat(columns.map{c => c.toExpr}: _*))
  }
  
  def smvAsArray(columns: Column*) = {
    new Column(SmvAsArray(columns.map{c => c.toExpr}: _*))
  }

  def smvCreateLookUp[S,D](map: Map[S,D])(implicit st: TypeTag[S], dt: TypeTag[D]) = {
    val func: S => Option[D] = {s => map.get(s)}
    udf(func)
  }
}
