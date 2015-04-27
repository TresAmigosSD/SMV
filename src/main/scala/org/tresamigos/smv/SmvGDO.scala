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

import org.apache.spark.sql.contrib.smv._

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.{Column, ColumnName}
import org.apache.spark.sql.GroupedData
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{BooleanType}

import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans.logical.LocalRelation
import org.apache.spark.sql.catalyst.dsl.plans._

import org.apache.spark.sql.catalyst.ScalaReflection
import scala.reflect.runtime.universe.{TypeTag, typeTag}
/*
import org.apache.spark.sql.catalyst.dsl.expressions._
*/

/**
 * SmvGDO - SMV GroupedData Operator
 **/

abstract class SmvGDO extends Serializable{
  def inGroupKeys: Seq[String]
  
  /* inGroupIterator in SmvCDS should be independent of what aggregation function will be performed, so 
   * it has too be very general and as a result, may not be very efficiant */
  def inGroupIterator(smvSchema:SmvSchema): Iterable[Row] => Iterable[Row]
  
  def outSchema(inSchema: SmvSchema): SmvSchema 
}
