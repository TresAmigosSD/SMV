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

import org.apache.spark.sql.types.{Metadata, MetadataBuilder}
import org.apache.spark.sql.DataFrame
import org.apache.spark.SparkContext

class SmvMetadata {
  val builder = new MetadataBuilder

  def addFQN(fqn: String) =
    builder.putString("fqn", fqn)

  def addSchemaMetadata(df: DataFrame) =
    builder.putMetadataArray("columns", createSchemaMetadataArray(df))

  // Schema metadata is organized in an array to preserve order of columns
  private def createSchemaMetadataArray(df: DataFrame): Array[Metadata] =
    SmvSchema.fromDataFrame(df).entries.map{ entry =>
      val field = entry.field
      val typeFormat = entry.typeFormat
      val colBuilder =
        new MetadataBuilder()
        .putString("type", typeFormat.typeName)
        .putString("name", field.name)
      if(typeFormat.format != null)
        colBuilder.putString("format", typeFormat.format)
      if(field.metadata != Metadata.empty)
        colBuilder.putMetadata("metadata", field.metadata)
      colBuilder.build
    }.toArray

  def toSparkMetadata: Metadata =
    builder.build

  override def toString =
    toSparkMetadata.json

  def saveToFile(sc: SparkContext, path: String) =
    sc.makeRDD(Seq(toString), 1).saveAsTextFile(path)
}
