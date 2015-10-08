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

package org.apache.spark.sql.contrib

import org.apache.spark.sql.{Row, Column}
import org.apache.spark.rdd.RDD

import org.apache.spark.sql.types._
import org.apache.spark.sql.catalyst.{InternalRow, CatalystTypeConverters}

/**
 * Since we need to access some of the private[sql] classes and methods,
 * we use this hack (not a trojan horse :-)) to give org.tresamigos.smv access
 **/
package object smv {
  def extractExpr(c: Column) = c.expr

  /** return Ordering[Any] to compare values of Any */
  def getOrdering[T<: DataType](t: T): Ordering[Any] = {
    t match {
      case v: AtomicType => v.ordering.asInstanceOf[Ordering[Any]]
      case v => throw new IllegalArgumentException(s"DataType: $v has no ordering")
    }
  }

  /** return Numeric[Any] for the NumericType*/
  def getNumeric[T<: DataType](t: T): Numeric[Any] = {
    t match {
      case v: NumericType => v.numeric.asInstanceOf[Numeric[Any]]
      case v => throw new IllegalArgumentException(s"DataType: $v has no numeric")
    }
  }

  /** give access to StructType merge method */
  def mergeStructType(left: StructType, right: StructType): StructType = {
    left.merge(right)
  }

  def convertToCatalyst(rowRDD: Iterable[Row], schema: StructType) = {
    val converter = CatalystTypeConverters.createToCatalystConverter(schema)
    rowRDD.map(converter(_).asInstanceOf[InternalRow])
  }

  def convertToScala(rowRDD: Iterable[InternalRow], schema: StructType) = {
    val converter = CatalystTypeConverters.createToScalaConverter(schema)
    rowRDD.map(converter(_).asInstanceOf[Row])
  }
}
