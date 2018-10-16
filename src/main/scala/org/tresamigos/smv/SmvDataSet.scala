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

import org.apache.spark.sql.{DataFrame, SaveMode}

import org.apache.hadoop.fs.FileStatus

import dqm.{DQMValidator, DqmValidationResult, ParserLogger, SmvDQM, TerminateParserLogger, FailParserCountPolicy}

import scala.collection.JavaConversions._
import scala.util.{Try, Success, Failure}

import java.io.FileNotFoundException

import edd._

import org.joda.time._, format._

/**
 * Dependency management unit within the SMV application framework.  Execution order within
 * the SMV application framework is derived from dependency between SmvDataSet instances.
 * Instances of this class can either be a file or a module. In either case, there would
 * be a single result DataFrame.
 */
abstract class SmvDataSet {

  def app: SmvApp                                                = SmvApp.app

  /** Cache metadata so we can reuse it between validation and persistence */
  private var userMetadataCache: Option[SmvMetadata]             = None

  /**
    * Cache the user metadata result (call to `metadata(df)`) so that multiple calls
    * to this `getOrCreateMetadata` method will only do a single evaluation of the user
    * defined metadata method which could be quite expensive.
    */
  private def getOrCreateUserMetadata(df: DataFrame): SmvMetadata = {
    userMetadataCache match {
      // use cached metadata if it exists
      case Some(meta) => meta
      // calculate and cache user metadata otherwise
      case None => {
        def _createUserMetadata() = metadata(df)
        val (userMetadata, elapsed) = doAction(f"GENERATE USER METADATA")(_createUserMetadata)
        userMetadataCache = Some(userMetadata)
        userMetadataTimeElapsed = Some(elapsed)
        userMetadata
      }
    }
  }

  /**
   * The FQN of an SmvDataSet is its classname for Scala implementations.
   *
   * Scala proxies for implementations in other languages must
   * override this to name the proxied FQN.
   */
  def fqn: String       = this.getClass().getName().filterNot(_ == '$')
  def urn: URN          = ModURN(fqn)
  override def toString = urn.toString

  def description(): String

  /** DataSet type: could be 4 values, Input, Link, Module, Output */
  def dsType(): String

  /** modules must override to provide set of datasets they depend on.
   * This is no longer the canonical list of dependencies. Internally
   * we should query resolvedRequiresDS for dependencies.
   */
  def requiresDS(): Seq[SmvDataSet]

  /** fixed list of SmvDataSet dependencies */
  var resolvedRequiresDS: Seq[SmvDataSet] = Seq.empty[SmvDataSet]

  /**
   * Timestamp which will be included in the metadata. Should be the timestamp
   * of the transaction that loaded this module.
   */
  private var timestamp: Option[DateTime] = None

  private[smv] def setTimestamp(dt: DateTime) =
    timestamp = Some(dt)

  private var userMetadataTimeElapsed: Option[Double] = None
  private var persistingTimeElapsed: Option[Double]   = None
  private var totalTimeElapsed: Option[Double]        = None
  private var dqmTimeElapsed: Option[Double]          = None

  lazy val ancestors: Seq[SmvDataSet] =
    (resolvedRequiresDS ++ resolvedRequiresDS.flatMap(_.ancestors)).distinct

  def resolve(resolver: DataSetResolver): SmvDataSet = {
    resolvedRequiresDS = requiresDS map (resolver.resolveDataSet(_))
    this
  }

  /** user tagged code "version".  Derived classes should update the value when code or data */
  def version(): Int = 0

  /** full name of hive output table if this module is published to hive. */
  def tableName: String = throw new IllegalStateException("tableName not specified for ${fqn}")

  /** Objects defined in Spark Shell has class name start with $ **/
  private val isObjectInShell: Boolean = this.getClass.getName matches """\$.*"""

  /** Hash computed from the dataset, could be overridden to include things other than CRC */
  def datasetHash(): Int = {
    val _instanceValHash = instanceValHash
    app.log.debug(f"${fqn}.instanceValHash = ${_instanceValHash}")

    val _sourceCodeHash = sourceCodeHash
    app.log.debug(f"${fqn}.sourceCodeHash = ${_sourceCodeHash}")

    _instanceValHash + _sourceCodeHash
  }
  /** Hash computed based on instance values of the dataset, such as the timestamp of an input file **/
  def instanceValHash(): Int = 0
  /** Hash computed based on the source code of the dataset's class **/
  def sourceCodeHash(): Int

  /**
   * Determine the hash of this module and the hash of hash (HOH) of all the modules it depends on.
   * This way, if this module or any of the modules it depends on changes, the HOH should change.
   * The "version" of the current dataset is also used in the computation to allow user to force
   * a change in the hash of hash if some external dependency changed as well.
   */
  private[smv] lazy val hashOfHash: Int = {
    val _datasetHash = datasetHash
    app.log.debug(f"${fqn}.datasetHash = ${_datasetHash}")
    
    val res = (resolvedRequiresDS.map(_.hashOfHash) ++ Seq(version, datasetHash)).hashCode()
    app.log.debug(f"${fqn}.hashOfHash = ${res}")
    
    res
  }

  /**
   * Since hashOfHash is lazy, persistStgy also lazy
   **/
  private[smv] lazy val persistStgy = new DfCsvOnHdfsStgy(
    app, fqn, hashOfHash
  )

  /**
   * flag if this module is ephemeral or short lived so that it will not be persisted when a graph is executed.
   * This is quite handy for "filter" or "map" type modules so that we don't force an extra I/O step when it
   * is not needed.  By default all modules are persisted unless the flag is overridden to true.
   * Note: the module will still be persisted if it was specifically selected to run by the user.
   */
  def isEphemeral: Boolean

  /**
   * An optional sql query to run to publish the results of this module when the
   * --publish-hive command line is used.  The DataFrame result of running this
   * module will be available to the query as the "dftable" table.  For example:
   *    return "insert overwrite table mytable select * from dftable"
   * If this method is not specified, the default is to just create the table
   * specified by tableName() with the results of the module.
   */
  def publishHiveSql: Option[String] = None

  /**
   * Exports a dataframe to a hive table.
   */
  def exportToHive(collector: SmvRunInfoCollector) = {
    val dataframe = rdd(collector=collector)

    // register the dataframe as a temp table.  Will be overwritten on next register.
    dataframe.createOrReplaceTempView("dftable")

    // if user provided a publish hive sql command, run it instead of default
    // table creation from data frame result.
    val queries: Seq[String] = publishHiveSql match {
      case Some(query) => query.split(";") map (_.trim)
      case None        => Seq(s"drop table if exists ${tableName}",
                              s"create table ${tableName} as select * from dftable")
    }
    app.log.info(f"Hive publish query: ${queries.mkString(";")}")
    doAction(f"PUBLISH TO HIVE") {queries foreach {app.sqlContext.sql}}
  }

  /** do not persist validation result if isObjectInShell **/
  private def isPersistValidateResult = !isObjectInShell

  /**
   * Define the DQM rules, fixes and policies to be applied to this `DataSet`.
   * See [[org.tresamigos.smv.dqm]], [[org.tresamigos.smv.dqm.DQMRule]], and [[org.tresamigos.smv.dqm.DQMFix]]
   * for details on creating rules and fixes.
   *
   * Concrete modules and files should override this method to define rules/fixes to apply.
   * The default is to provide an empty set of DQM rules/fixes.
   */
  def dqm(): SmvDQM =
    SmvDQM()

  /**
   * Allow internal SMV DataSet types to add additional policy checks to user specified DQM rules.
   * Note: we should accept the user DQM rules as param rather than call dqm() directly as
   * we may need to be passed the user defined DQM rules in python.
   */
  private def dqmWithTypeSpecificPolicy(userDQM: SmvDQM) =
    userDQM

  /**
   * returns the DataFrame from this dataset (file/module).
   * The value is cached so this function can be called repeatedly. The cache is
   * external to SmvDataSet so that it we will not recalculate the DF even after
   * dynamically loading the same SmvDataSet. If force argument is true, the we
   * skip the cache.
   * Note: the RDD graph is cached and NOT the data (i.e. rdd.cache is NOT called here)
   *
   * @param forceRun skip the module cache and for the module to run
   * @param genEdd if true, compute and persist EDD to file
   * @param collector recepticle for info about running the module
   * @param quickRun skip computing metadata+validation and persisting output
   */
  def rdd(forceRun: Boolean = false,
          genEdd: Boolean = app.genEdd,
          collector: SmvRunInfoCollector,
          quickRun: Boolean = false) = {
    if (forceRun || !app.dfCache.contains(versionedFqn)) {
      app.dfCache = app.dfCache + (versionedFqn -> computeDataFrame(genEdd, collector, quickRun))
    }
    app.dfCache(versionedFqn)
  }

  def verHex: String = f"${hashOfHash}%08x"
  def versionedFqn   = s"${fqn}_${verHex}"

  /** The "versioned" module file base name. */
  private def versionedBasePath(): String = {
    s"""${app.smvConfig.outputDir}/${versionedFqn}"""
  }

  /** Returns the path for the module's csv output */
  def moduleCsvPath(): String =
    versionedBasePath() + ".csv"

  /** Returns the path for the module's schema file */
  private[smv] def moduleSchemaPath(): String =
    versionedBasePath() + ".schema"

  /** Returns the path for the module's edd report output */
  private def moduleEddPath(): String =
    versionedBasePath() + ".edd"

  /** Returns the path for the module's reject report output */
  private def moduleValidPath(): String =
    versionedBasePath() + ".valid"

  /** Returns the path for the module's metadata output */
  private[smv] def moduleMetaPath(): String =
    versionedBasePath() + ".meta"

  /** Returns the path for the module's metadata history */
  private def moduleMetaHistoryPath(): String =
    s"""${app.smvConfig.historyDir}/${fqn}.hist"""

  /** perform the actual run of this module to get the generated SRDD result. */
  private[smv] def doRun(dqmValidator: DQMValidator, collector: SmvRunInfoCollector, quickRun: Boolean): DataFrame

  /**
   * delete the output(s) associated with this module (csv file and schema).
   * TODO: replace with df.write.mode(Overwrite) once we move to spark 1.4
   */
  private def deleteFiles(files: Seq[String]) =
     files foreach {SmvHDFS.deleteFile}

  /* TODO: use persistStrg */
  private[smv] def deleteOutputs() =
    deleteFiles(versionedOutputFiles)

  /**
   * Delete just the metadata output
   */
  private def deleteMetadataOutputs() =
    deleteFiles(metadataOutputFiles)

  /**
   * Files related to metadata
   */
  private def metadataOutputFiles(): Seq[String] =
    Seq(moduleMetaPath(), moduleMetaHistoryPath())

  /**
   * Returns current all outputs produced by this module.
   */
  private[smv] def allOutputFiles(): Seq[String] = {
    Seq(moduleCsvPath(), moduleSchemaPath(), moduleEddPath(), moduleValidPath(), moduleMetaPath(), moduleMetaHistoryPath())
  }

  /**
   * Returns current versioned outputs produced by this module. Excludes metadata history
   */
  private def versionedOutputFiles(): Seq[String] = {
    Seq(moduleCsvPath(), moduleSchemaPath(), moduleEddPath(), moduleValidPath(), moduleMetaPath())
  }

  /**
   * Perform an action and log the amount of time it took
   */
  private def doAction[T](desc: String)(action: => T): (T, Double) = 
      app.doAction(desc, fqn)(action)

  private def readPersistedMetadata(): Try[SmvMetadata] =
    Try {
      val json = app.sc.textFile(moduleMetaPath()).collect.head
      SmvMetadata.fromJson(json)
    }

  private def readMetadataHistory(): Try[SmvMetadataHistory] =
    Try {
      val json = app.sc.textFile(moduleMetaHistoryPath()).collect.head
      SmvMetadataHistory.fromJson(json)
    }

  private def readPersistedEdd(): Try[DataFrame] =
    Try { app.sqlContext.read.json(moduleEddPath()) }

  /**
   * #560
   *
   * Make this a `lazy val` to avoid O(n^2) when each module triggers
   * computation in all its ancestors.
   */
  private[smv] lazy val needsToRun: Boolean = {
    val upstreamNeedsToRun = resolvedRequiresDS.exists(_.needsToRun)
    if (upstreamNeedsToRun)
      true
    else if (isEphemeral)
      false
    else
      !persistStgy.isPersisted
  }

  /**
   * Read EDD from disk if it exists, or create and persist it otherwise
   */
  private def getEdd(collector: SmvRunInfoCollector): String = {
    // DON'T automatically persist edd. Edd is explicitly persisted on the next
    // line. This is the simplest way to prevent EDD from being persisted twice.
    val df = rdd(forceRun = false, genEdd = false, collector=collector)

    val unorderedSummary = readPersistedEdd().getOrElse {
      persistEdd(df)
      readPersistedEdd().get
      // The persisted df's columns will be ordered arbitrarily, and need to be
      // reordered to be a valid edd result
    }.select(EddResult.resultSchema.head, EddResult.resultSchema.tail: _*)

    // Summary rows will be ordered arbitrarily after persisting. Need
    // to reorder according to the columns of the df. Original order of tasks will
    // still most likely be lost
    val orderedSummary = df.columns.map { dfColName =>
      unorderedSummary.filter(unorderedSummary("colName") === dfColName)
    }.reduce {
      _.smvUnion(_)
    }

    EddResultFunctions(orderedSummary).createReport()
  }

  private def persistEdd(df: DataFrame) =
    df.edd.persistBesideData(moduleCsvPath())

  /**
   * Can be overridden to supply custom metadata
   * TODO: make SmvMetadata more user friendly or find alternative format for user metadata
   */
  def metadata(df: DataFrame): SmvMetadata =
    new SmvMetadata()

  /**
   * Get the most detailed metadata available without running this module. If
   * the modules has been run and hasn't been changed, this will be all the metadata
   * that was persisted. If the module hasn't been run since it was changed, this
   * report will exclude the DQM validation result and user metadata.
   */
  private[smv] def getMetadata(): SmvMetadata =
    readPersistedMetadata().getOrElse(getOrCreateMetadata(None, None))

  /**
   * Read metadata history from file if it exists, otherwise return empty metadata
   */
  private[smv] def getMetadataHistory(): SmvMetadataHistory =
    readMetadataHistory().getOrElse(SmvMetadataHistory.empty)

  /**
   * Create SmvMetadata for this SmvDataset. SmvMetadata will include user metadata
   * if dfOpt is not None, and will contain a jsonification of the DqmValidationResult
   * (including DqmState) if validResOpt is not None.
   *
   * Note: we only cache the user metadata result and not the entire `SmvMetadata`
   * return incase this method is called multiple times in same run with/without
   * the df argument.  We assume the df value is the same for all calls!
   */
  private def getOrCreateMetadata(dfOpt:       Option[DataFrame],
                                       validResOpt: Option[DqmValidationResult]): SmvMetadata = {
    val resMetadata = dfOpt match {
      // if df provided, get user metadata (which may be cached)
      case Some(df) => getOrCreateUserMetadata(df)
      // otherwise, skip user metadata - none can be evaluated if there if we haven't run the module
      case _        => new SmvMetadata()
    }
    resMetadata.addFQN(fqn)
    resMetadata.addDependencyMetadata(resolvedRequiresDS)
    resMetadata.addApplicationContext(app)
    dfOpt foreach {resMetadata.addSchemaMetadata}
    timestamp foreach {resMetadata.addTimestamp}
    validResOpt foreach {resMetadata.addDqmValidationResult}
    userMetadataTimeElapsed foreach {resMetadata.addDuration("metadata", _)}
    persistingTimeElapsed foreach {resMetadata.addDuration("persisting", _)}
    dqmTimeElapsed foreach {resMetadata.addDuration("dqm", _)}
    resMetadata
  }

  /**
   * Persist versioned copy of metadata
   */
  private def persistMetadata(metadata: SmvMetadata): Unit = {
    val metaPath =  moduleMetaPath()
    doAction(f"PERSIST METADATA") {metadata.saveToFile(app.sc, metaPath)}
    app.log.info(f"Metadata path: ${metaPath}")
  }
  /**
   * Maximum of the metadata history
   * TODO: Verify that this is positive
   */
  private[smv] def metadataHistorySize(): Integer = 5

  /**
   * Save metadata history with new metadata
   */
  private def persistMetadataHistory(metadata: SmvMetadata, oldHistory: SmvMetadataHistory): Unit = {
    val metaHistoryPath = moduleMetaHistoryPath()
    doAction(f"PERSIST METADATA HISTORY") {
      oldHistory
        .update(metadata, metadataHistorySize)
        .saveToFile(app.sc, metaHistoryPath)
    }
    app.log.info(f"Metadata history path: ${metaHistoryPath}")
  }
  /**
   * Override to validate module results based on current and historic metadata.
   * If Some, DQM will fail. Defaults to None.
   */
  def validateMetadata(metadata: SmvMetadata, history: Seq[SmvMetadata]): Option[String] =
    None

  /**
   * Compute and return the output DataFrame. Also, compute metadata and validate
   * DQ. Persist all results.
   *
   * @param genEdd if true, compute and persist EDD to file
   * @param collector recepticle for info about running the module
   * @param quickRun skip computing metadata+validation and persisting output
   */
  private def computeDataFrame(genEdd: Boolean,
                                    collector: SmvRunInfoCollector,
                                    quickRun: Boolean): DataFrame = {
    val dqmValidator  = new DQMValidator(dqmWithTypeSpecificPolicy(dqm), isPersistValidateResult)

    // shared logic when running ephemeral and non-ephemeral modules
    def runDqmAndMeta(df: DataFrame, hasAction: Boolean): Unit = {
      val (validationResult, validationDuration) =
        doAction(f"VALIDATE DATA QUALITY") {dqmValidator.validate(df, hasAction, moduleValidPath())}
      dqmTimeElapsed = Some(validationDuration)

      val metadata = getOrCreateMetadata(Some(df), Some(validationResult))
      val metadataHistory = getMetadataHistory
      // Metadata is complete at time of validation, including user metadata and validation result
      val validationRes: Option[String] = validateMetadata(metadata, metadataHistory.historyList)
      validationRes foreach {msg => throw new SmvMetadataValidationError(msg)}
      

      deleteFiles(metadataOutputFiles)
      persistMetadata(metadata)
      persistMetadataHistory(metadata, metadataHistory)

      collector.addRunInfo(fqn, validationResult, metadata, metadataHistory)
    }

    if (quickRun) {
      app.log.info(f"Skipping DQM, metadata, and caching for ${fqn}")
      doRun(dqmValidator, collector, quickRun)
    } else if (isEphemeral) {
      app.log.info(f"${fqn} is not cached, because it is ephemeral")
      val df = dqmValidator.attachTasks(doRun(dqmValidator, collector, quickRun))
      runDqmAndMeta(df, false) // no action before this point
      df
    } else {
      persistStgy.unPersist() match {
        // Output was persisted, so return a DF which consists of reading from
        // that persisted result
        case Success(df) =>
          app.log.info(f"Relying on cached result ${moduleCsvPath()} for ${fqn}")
          df
        // Output was not persisted (or something else went wrong), run module, persist output,
        // and return DFn which consists of reading from that persisted result
        case _ =>
          // Acquire lock on output CSV to ensure write is atomic
          persistStgy.withLock() {
            // Another process may have persisted the data while we
            // waited for the lock. So we read again before computing.
            persistStgy.unPersist() match {
              case Success(df) =>
                app.log.info(f"Relying on cached result ${moduleCsvPath()} for ${fqn} found after lock acquired")
                df
              case _ =>
                app.log.info(f"No cached result found for ${fqn}. Caching result at ${moduleCsvPath()}")
                val df = dqmValidator.attachTasks(doRun(dqmValidator, collector, quickRun))
                // Delete outputs in case data was partially written previously
                persistStgy.rmPersisted()
                persistingTimeElapsed = Some(persistStgy.persist(df))

                runDqmAndMeta(df, true) // has already had action (from persist)

                // Generate and persist edd based on result of reading results from disk. Avoids
                // a possibly expensive action on the result from before persisting.
                if(genEdd)
                  persistEdd(df)
                persistStgy.unPersist().get
            }
          }
      }
    }
  }

  /**
   * Returns the run information from this dataset's last run.
   *
   * If the dataset has never been run, returns an empty run info with
   * null for its components.
   */
  def runInfo: SmvRunInfo = {
    val validation = DQMValidator.readPersistedValidationFile(moduleValidPath()).toOption.orNull
    val meta = readPersistedMetadata().toOption.orNull
    val mhistory = readMetadataHistory().toOption.orNull
    SmvRunInfo(validation, meta, mhistory)
  }

  /** path to published output without file extension **/
  private def publishPathNoExt(version: String) = s"${app.smvConfig.publishDir}/${version}/${fqn}"

  /** path of published data for a given version. */
  private def publishCsvPath(version: String) = publishPathNoExt(version) + ".csv"

  /** path of published metadata for a given version */
  private def publishMetaPath(version: String) = publishPathNoExt(version) + ".meta"

  /** path of published metadata history for a given version */
  private def publishHistoryPath(version: String) = publishPathNoExt(version) + ".hist"

  /**
   * Publish the current module data to the publish directory.
   * PRECONDITION: user must have specified the --publish command line option (that is where we get the version)
   */
  private[smv] def publish(collector: SmvRunInfoCollector) = {
    val df      = rdd(collector=collector)
    val version = app.smvConfig.publishVersion
    persistStgy.publish(df, version)
    // Read persisted metadata and metadata history, and publish it with the output.
    // Note that the metadata will have been persisted, because either
    // 1. the metadata was persisted before publish was started
    // 2. the module had not been run successully yet, so the module was run
    //    when rdd was called above, persisting the metadata.
    getMetadata.saveToFile(app.sc, publishMetaPath(version))
    getMetadataHistory.saveToFile(app.sc, publishHistoryPath(version))
    /* publish should also calculate edd if generarte Edd flag was turned on */
    if (app.genEdd)
      df.edd.persistBesideData(publishCsvPath(version))
  }

  /**
   * Publish DataFrame result using JDBC. Url will be user-specified.
   */
  private[smv] def publishThroughJDBC(collector: SmvRunInfoCollector) = {
    val df = rdd(collector=collector)
    val connectionProperties = new java.util.Properties()
    connectionProperties.put("driver", app.smvConfig.jdbcDriver)
    val url = app.smvConfig.jdbcUrl
    df.write.mode(SaveMode.Append).jdbc(url, tableName, connectionProperties)
  }

  private[smv] def parentStage: Option[String] = urn.getStage

}


/**
 * Concrete SmvDataSet representation of modules defined in Python. Created
 * exclusively by DataSetRepoPython. Wraps an ISmvModule.
 */
class SmvExtModulePython(target: ISmvModule) extends SmvDataSet with python.InterfacesWithPy4J {
  override val fqn            = getPy4JResult(target.getFqn)
  override val description    = getPy4JResult(target.getDescription)
  override def tableName      = getPy4JResult(target.getTableName)
  override def isEphemeral    = getPy4JResult(target.getIsEphemeral)
  override def publishHiveSql = Option(getPy4JResult(target.getPublishHiveSql))
  override def dsType         = getPy4JResult(target.getDsType)
  override def requiresDS     =
    throw new SmvRuntimeException("SmvExtModulePython requiresDS should never be called")

  override def resolve(resolver: DataSetResolver): SmvDataSet = {
    val urns = getPy4JResult(target.getDependencyUrns)
    resolvedRequiresDS = urns map (urn => resolver.loadDataSet(URN(urn)))
    this
  }

  override private[smv] def doRun(dqmValidator: DQMValidator,
                                  collector: SmvRunInfoCollector,
                                  quickRun: Boolean): DataFrame = {
    val urn2df: Map[String, DataFrame] =
      resolvedRequiresDS.map { ds => (ds.urn.toString, ds.rdd(collector=collector, quickRun=quickRun))}.toMap
    val response =  target.getDoRun(dqmValidator, urn2df)
    return getPy4JResult(response)
  }

  override def instanceValHash =
    getPy4JResult(target.getInstanceValHash)

  override def sourceCodeHash =
    getPy4JResult(target.getSourceCodeHash)

  override def dqm =
    getPy4JResult(target.getDqmWithTypeSpecificPolicy)

  override def metadata(df: DataFrame) = {
    val py4jRes = target.getMetadataJson(df)
    val json = getPy4JResult(py4jRes)
    SmvMetadata.fromJson(json)
  }

  override def validateMetadata(current: SmvMetadata, history: Seq[SmvMetadata]) = {
    val currentJson = current.toJson
    val historyJson = history map (_.toJson)
    val res = getPy4JResult(target.getValidateMetadataJson(currentJson, historyJson.toArray))
    Option[String](res)
  }

  override def metadataHistorySize() =
    getPy4JResult(target.getMetadataHistorySize)
}

/**
 * Factory for SmvExtModulePython. Creates an SmvExtModulePython with SmvOuptut
 * if the Python dataset is SmvOutput
 */
object SmvExtModulePython extends python.InterfacesWithPy4J {
  def apply(target: ISmvModule): SmvExtModulePython = {
    if (getPy4JResult(target.getIsOutput))
      new SmvExtModulePython(target) with SmvOutput
    else
      new SmvExtModulePython(target)
  }
}

/**
 * A marker trait that indicates that a SmvDataSet/SmvModule decorated with this trait is an output DataSet/module.
 */
trait SmvOutput { this: SmvDataSet =>
  override def dsType(): String = "Output"
}
