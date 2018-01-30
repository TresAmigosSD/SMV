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

import org.joda.time.DateTime


/**
 * Representation of module metadata which can be saved to file.
 *
 * TODO: Add getter methods and more types of metadata (e.g. validation results)
 */
class SmvMetadata(val builder: MetadataBuilder = new MetadataBuilder) {

  /**
   * Add FQN field
   */
  def addFQN(fqn: String) =
    builder.putString("_fqn", fqn)

  /**
   * Extract schema-related metadata from this DataFrame and add it
   */
  def addSchemaMetadata(df: DataFrame) =
    builder.putMetadataArray("_columns", createSchemaMetadataArray(df))

  /**
   * Add dependency-related metadata based on a list of dependencies
   */
  def addDependencyMetadata(deps: Seq[SmvDataSet]) = {
    val dependencyPaths = deps map ( _.moduleMetaPath() )
    builder.putStringArray("_inputs", dependencyPaths.toArray)
  }

  /**
   * Add timestamp for running the application to metadata
   */
  def addTimestamp(dt: DateTime) = {
    builder.putString("_timestamp", dt.toString)
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
  def toJson: String =
    builder.build.json

  /**
   * Saves the string representation to file as a single row RDD
   */
  def saveToFile(sc: SparkContext, path: String) =
    sc.makeRDD(Seq(toJson), 1).saveAsTextFile(path)
}

object SmvMetadata {
  def apply(sparkMetadata: Metadata): SmvMetadata = {
    val builder = new MetadataBuilder().withMetadata(sparkMetadata)
    new SmvMetadata(builder)
  }

  def fromJson(json: String): SmvMetadata = {
    val metadataFromString = Metadata.fromJson(json)
    val builder            = (new MetadataBuilder()).withMetadata(metadataFromString)
    new SmvMetadata(builder)
  }
}

/**
 * Interface for updating metadata history.
 * @param historyList Array of SmvMetadata in descending order of age
 */
class SmvMetadataHistory(val historyList: Array[SmvMetadata]) {
  def apply(idx: Integer): SmvMetadata =
    historyList(idx)

  /**
   * Get new SmvMetadataHistory updated with new metadata
   */
  def update(newMeta: SmvMetadata, maxSize: Integer): SmvMetadataHistory = {
    new SmvMetadataHistory(newMeta +: historyList.take(maxSize - 1))
  }

  /**
   * Serialize metadata as JSON string. Structure will be
   * {
   *    "history": [
   *      ...
   *    ]
   * }
   */
  def toJson: String =
    new MetadataBuilder()
      .putMetadataArray("history", historyList map (_.builder.build))
      .build
      .json

  def length: Integer =
    historyList.length

  def saveToFile(sc: SparkContext, path: String) =
    sc.makeRDD(Seq(toJson), 1).saveAsTextFile(path)
}

object SmvMetadataHistory {
  /**
   * Read history from JSON string
   */
  def fromJson(json: String): SmvMetadataHistory = {
    val metadataFromString = Metadata.fromJson(json)
    val smvMetadataArray = metadataFromString.getMetadataArray("history") map {SmvMetadata(_)}
    new SmvMetadataHistory(smvMetadataArray)
  }

  /**
   * Create a history with no entries
   */
  def empty(): SmvMetadataHistory = {
    new SmvMetadataHistory(Array.empty)
  }
}

/**
 * Policy for validating a module's current metadata against its historical
 * metadata. This policy is added to every module's DQM
 */
class DQMMetadataPolicy(ds: SmvDataSet) extends dqm.DQMPolicy{
  def name(): String =
    s"${ds.fqn} metadata validation"

  def policy(df: DataFrame, state: dqm.DQMState) = {
    val metadata = ds.getOrCreateMetadata(Some(df))
    val history = ds.getMetadataHistory()
    val result = ds.validateMetadata(metadata, history.historyList)
    result match {
      case Some(failMsg) =>
        // existence of failMsg indicates failure
        state.addMiscLog(failMsg)
        false
      case None      =>
        // no failure indicates success
        true
    }
  }
}
