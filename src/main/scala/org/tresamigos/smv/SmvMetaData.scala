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

/**
 * Representation of module metadata which can be saved to file.
 *
 * TODO: Add getter methods and more types of metadata (e.g. validation results)
 */
class SmvMetadata(builder: MetadataBuilder = new MetadataBuilder) {

  /**
   * Add FQN field
   */
  def addFQN(fqn: String) =
    builder.putString("fqn", fqn)

  /**
   * Extract schema-related metadata from this DataFrame and add it
   */
  def addSchemaMetadata(df: DataFrame) =
    builder.putMetadataArray("columns", createSchemaMetadataArray(df))

  /**
   * Add dependency-related metadata based on a list of dependencies
   */
  def addDependencyMetadata(deps: Seq[SmvDataSet]) = {
    val dependencyPaths = deps map ( _.moduleMetaPath() )
    builder.putStringArray("inputs", dependencyPaths.toArray)
  }
  
  /**
   * Returns an array where each element is a metadata containing information
   * about one of the DataFrame's columns, including name, type, and format and
   * column-level metadata if any. Order of the array is the order of the columns.
   */
  private def createSchemaMetadataArray(df: DataFrame): Array[Metadata] =
    SmvSchema
      .fromDataFrame(df)
      .entries
      .map { entry =>
        val field      = entry.field
        val typeFormat = entry.typeFormat
        val colBuilder =
          new MetadataBuilder()
            .putString("type", typeFormat.typeName)
            .putString("name", field.name)
        if (typeFormat.format != null)
          colBuilder.putString("format", typeFormat.format)
        if (field.metadata != Metadata.empty)
          colBuilder.putMetadata("metadata", field.metadata)
        colBuilder.build
      }
      .toArray

  /**
   * String representation is a minified json string
   */
  def toJson =
    builder.build.json

  /**
   * Saves the string representation to file as a single row RDD
   */
  def saveToFile(sc: SparkContext, path: String) =
    sc.makeRDD(Seq(toJson), 1).saveAsTextFile(path)
}

object SmvMetadata {
  def fromJson(json: String): SmvMetadata = {
    val metadataFromString = Metadata.fromJson(json)
    val builder            = (new MetadataBuilder()).withMetadata(metadataFromString)
    new SmvMetadata(builder)
  }
}
