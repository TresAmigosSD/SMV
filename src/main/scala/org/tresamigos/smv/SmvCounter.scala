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

import org.apache.spark.{Accumulable, AccumulableParam}
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import scala.collection.mutable.{Map => MutableMap}

/** 
 * SmvCounter is a general logger which wrapped around an accumulable of a mutable Map
 */
abstract class SmvCounter extends Serializable {
  val add: String => Unit = {name:String => Unit}
  def report: Map[String, Long] = Map()
  def reset(): Unit = {}
  def apply(name: String): Long = report.getOrElse(name, 0L)
}

object NoOpCounter extends SmvCounter 

class ScCounter(sparkContext: SparkContext) extends SmvCounter {

  implicit def histParam[T] = new AccumulableParam[MutableMap[T, Long], T]{
    def zero(initialValue: MutableMap[T, Long]): MutableMap[T, Long] = {
      MutableMap[T, Long]()
    }

    def addAccumulator(map: MutableMap[T, Long], newValue: T): MutableMap[T, Long] = {
      map += (newValue -> (map.getOrElse(newValue, 0L) + 1L))
    }

    def addInPlace(v1: MutableMap[T, Long], v2: MutableMap[T, Long]): MutableMap[T, Long] = {
      (v1 /: v2){case (map, (k, v)) => map += (k->(map.getOrElse(k,0L) + v))} 
    }
  }

  private val records = sparkContext.accumulable[MutableMap[String, Long], String](MutableMap[String, Long]())

  override val add: String => Unit = {name => 
    records += name
    Unit
  }
 
  override def report: Map[String, Long] = records.value.toMap
  
  override def reset() = {
    records.setValue(MutableMap[String, Long]())
    Unit
  }
}

