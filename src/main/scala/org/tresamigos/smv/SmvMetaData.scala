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
import org.apache.spark.{SparkContext, SparkConf}

import org.joda.time.DateTime

import dqm.DqmValidationResult


/**
 * Representation of module metadata which can be saved to file.
 *
 * TODO: Add getter methods and more types of metadata (e.g. validation results)
 */
class SmvMetadata(val builder: MetadataBuilder = new MetadataBuilder) {
  // Durations of task involved in running a module, by bame
  var task2duration: Map[String, Double] = Map.empty

  def getDurationMeta: Option[Metadata] = {
    if (task2duration.size > 0) {
      val durationBuilder = new MetadataBuilder
      task2duration foreach {case (task, duration) => durationBuilder.putDouble(task, duration)}
      Some(durationBuilder.build)
    } else {
      // exclude duration entry from metadata if there are no task durations to include
      // this will happen when the module has not been run
      None
    }

  }

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
   * Add description of how many seconds a task took to run
   */
  def addDuration(name: String, duration: Double) = {
    task2duration = task2duration + (name -> duration)
  }

  /**
   * Add ID of application in the resource manager - this is provided by Spark, and
   * the format varies between local, standalone, YARN, and Mesos
   * see TODO: add link to spark api doc
   */
  def addApplicationId(applicationId: String) = {
    builder.putString("_applicationId", applicationId)
  }

  /**
   * Add validation result (including DQM state) to metadata
   */
   def addDqmValidationResult(result: DqmValidationResult) = {
     /**TODO: this is roundabout, since we are just going to re-jsonify the
      * the metadata when we write it. It will do for now, and we should probably
      * rewrite SmvMetadata to omit Spark Metadata anyway, seeing as it doesn't
      * support null values (issue #1138).
      */
     addJson("_validation", result.toJSON)
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
   * Add config and version information which is part of application context
   */
  def addApplicationContext(smvApp: SmvApp) = {
    addSmvConfig(smvApp.smvConfig)
    addSparkConfig(smvApp.sc.getConf)
    addSparkVersion(smvApp.sc.version)
    addApplicationId(smvApp.sc.applicationId)
  }

  /**
   * Add SmvConfig as Json object of KVs
   */
  def addSmvConfig(config: SmvConfig) = {
    addConfigMap("_smvConfig", config.mergedProps)
  }

  /**
   * Add SparkConfig as Json object of KVs
   */
  def addSparkConfig(config: SparkConf) = {
    val configTuples: Array[(String, String)] = config.getAll
    val configMap = Map[String, String](configTuples: _*)
    addConfigMap("_sparkConfig", configMap)
  }

  /**
   * Add version of Spark which SMV is deployed with
   */
  def addSparkVersion(version: String) = {
    builder.putString("_sparkVersion", version)
  }

  def addConfigMap(key: String, config: Map[String, String]) = {
    val configMetaBuilder = new MetadataBuilder
    config foreach { case ((k, v)) => configMetaBuilder.putString(k, v) }
    val configMeta = configMetaBuilder.build
    builder.putMetadata(key, configMeta)
  }

  /**
   * Add a Metadata object based on a json string
   */
  private def addJson(key: String, json: String) = {
    val metadata = Metadata.fromJson(json)
    builder.putMetadata(key, metadata)
  }

  /**
   * String representation is a minified json string
   */
  def toJson: String = {
    getDurationMeta foreach {builder.putMetadata("_duration", _)}
    builder.build.json
  }

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
