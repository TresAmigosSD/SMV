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

import SmvKeys.{SmvLabel, SmvDesc}

import org.apache.spark.sql.contrib.smv._
import org.apache.spark.sql.types._
import scala.util.Try

private[smv] class StructFieldHelper(field: StructField) {
  def ordering() = getOrdering(field.dataType)
  def numeric() = getNumeric(field.dataType)

  def labels: Seq[String] = {
    val meta = field.metadata
    if (meta.contains(SmvLabel)) meta.getStringArray(SmvLabel).toSeq else Seq.empty
  }
}

private[smv] class StructTypeHelper(schema: StructType) {
  def mergeSchema(that: StructType) = mergeStructType(schema, that)
  def getIndices(names: String*) = names.map{n => schema.fieldNames.indexOf(n)}
  def selfJoined(): StructType = {
    val renamed = schema.fields.map{f => StructField("_" + f.name, f.dataType, f.nullable)}
    StructType(schema.fields ++ renamed)
  }

  def getDescs(): Seq[(String, String)] = {
    schema.fields.map{f =>
      val md = Try(f.metadata.getString(SmvDesc)).recoverWith{case e => Try("")}.get
      (f.name, md)
    }.toSeq
  }
}
