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
  def getOrCreateUserMetadata(df: DataFrame): SmvMetadata = {
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

  def setTimestamp(dt: DateTime) =
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

  /** All dependencies with the dependency hierarchy flattened */
  def allDeps: Seq[SmvDataSet] =
    (resolvedRequiresDS
      .foldLeft(Seq.empty[SmvDataSet]) { (acc, elem) =>
        elem.allDeps ++ (elem +: acc)
      })
      .distinct

  /** user tagged code "version".  Derived classes should update the value when code or data */
  def version(): Int = 0

  /** full name of hive output table if this module is published to hive. */
  def tableName: String = throw new IllegalStateException("tableName not specified for ${fqn}")

  /** Objects defined in Spark Shell has class name start with $ **/
  val isObjectInShell: Boolean = this.getClass.getName matches """\$.*"""

  /** Hash computed from the dataset, could be overridden to include things other than CRC */
  def datasetHash(): Int = instanceValHash + sourceCodeHash
  /** Hash computed based on instance values of the dataset, such as the timestamp of an input file **/
  def instanceValHash(): Int = 0
  /** Hash computed based on the source code of the dataset's class **/
  def sourceCodeHash(): Int

  /**
   * Determine the hash of this module and the hash of hash (HOH) of all the modules it depends on.
   * This way, if this module or any of the modules it depends on changes, the HOH should change.
   * The "version" of the current dataset is also used in the computation to allow user to force
   * a change in the hash of hash if some external dependency changed as well.
   * TODO: need to add requiresAnc dependency here
   */
  private[smv] lazy val hashOfHash: Int = {
    (resolvedRequiresDS.map(_.hashOfHash) ++ Seq(version, datasetHash)).hashCode()
  }

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
  private[smv] def isPersistValidateResult = !isObjectInShell

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
  private[smv] def dqmWithTypeSpecificPolicy(userDQM: SmvDQM) =
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
  private def versionedBasePath(prefix: String): String = {
    s"""${app.smvConfig.outputDir}/${prefix}${versionedFqn}"""
  }

  /** Returns the path for the module's csv output */
  def moduleCsvPath(prefix: String = ""): String =
    versionedBasePath(prefix) + ".csv"

  def lockfilePath(prefix: String = ""): String =
    moduleCsvPath(prefix) + ".lock"

  /** Returns the file status for the lockfile if found */
  def lockfileStatus: Option[FileStatus] =
    // use try/catch instead of Try because we want to handle only
    // FileNotFoundException and let other errors bubble up
    try {
      Some(SmvHDFS.getFileStatus(lockfilePath()))
    } catch {
      case e: FileNotFoundException => None
    }

  /** Returns the path for the module's schema file */
  private[smv] def moduleSchemaPath(prefix: String = ""): String =
    versionedBasePath(prefix) + ".schema"

  /** Returns the path for the module's edd report output */
  private[smv] def moduleEddPath(prefix: String = ""): String =
    versionedBasePath(prefix) + ".edd"

  /** Returns the path for the module's reject report output */
  private[smv] def moduleValidPath(prefix: String = ""): String =
    versionedBasePath(prefix) + ".valid"

  /** Returns the path for the module's metadata output */
  private[smv] def moduleMetaPath(prefix: String = ""): String =
    versionedBasePath(prefix) + ".meta"

  /** Returns the path for the module's metadata history */
  private[smv] def moduleMetaHistoryPath(prefix: String = ""): String =
    s"""${app.smvConfig.historyDir}/${prefix}${fqn}.hist"""

  /** perform the actual run of this module to get the generated SRDD result. */
  private[smv] def doRun(dqmValidator: DQMValidator, collector: SmvRunInfoCollector, quickRun: Boolean): DataFrame

  /**
   * delete the output(s) associated with this module (csv file and schema).
   * TODO: replace with df.write.mode(Overwrite) once we move to spark 1.4
   */
  private[smv] def deleteOutputs(files: Seq[String]) =
     files foreach {SmvHDFS.deleteFile}

  /**
   * Delete just the metadata output
   */
  private[smv] def deleteMetadataOutputs() =
    deleteOutputs(metadataOutputFiles)

  /**
   * Files related to metadata
   */
  private[smv] def metadataOutputFiles(): Seq[String] =
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
  private[smv] def versionedOutputFiles(): Seq[String] = {
    Seq(moduleCsvPath(), moduleSchemaPath(), moduleEddPath(), moduleValidPath(), moduleMetaPath())
  }

  /**
   * Read a dataframe from a persisted file path, that is usually an
   * input data set or the output of an upstream SmvModule.
   *
   * The default format is headerless CSV with '"' as the quote
   * character
   */
  def readFile(path: String,
               attr: CsvAttributes = CsvAttributes.defaultCsv): DataFrame =
    new FileIOHandler(app.sparkSession, path).csvFileWithSchema(attr)

  /**
   * Perform an action and log the amount of time it took
   */
  def doAction[T](desc: String)(action: => T): (T, Double)= {
    val fullDesc = f"${desc}: ${fqn}"
    app.log.info(f"STARTING ${fullDesc}")
    app.sc.setJobGroup(groupId=fqn, description=desc)
    val before  = DateTime.now()

    val res: T = action

    val after   = DateTime.now()
    val duration = new Duration(before, after)
    val secondsElapsed = duration.getMillis() / 1000.0

    val runTimeStr = PeriodFormat.getDefault().print(duration.toPeriod)
    app.sc.clearJobGroup()
    app.log.info(s"COMPLETED ${fullDesc}")
    app.log.info(s"RunTime: ${runTimeStr}")

    (res, secondsElapsed)
  }

  def persist(dataframe: DataFrame,
              prefix: String = ""): Unit = {
    val path = moduleCsvPath(prefix)
    val fmt = DateTimeFormat.forPattern("HH:mm:ss")

    val counter = app.sparkSession.sparkContext.longAccumulator
    val before  = DateTime.now()

    val df      = dataframe.smvPipeCount(counter)
    val handler = new FileIOHandler(app.sparkSession, path)

    val (_, elapsed) =
      doAction("PERSIST OUTPUT"){handler.saveAsCsvWithSchema(df, strNullValue = "_SmvStrNull_")}
    app.log.info(f"Output path: ${path}")
    persistingTimeElapsed = Some(elapsed)

    val n       = counter.value
    app.log.info(f"N: ${n}")
  }

  private[smv] def readPersistedFile(prefix: String = ""): Try[DataFrame] =
    Try(readFile(moduleCsvPath(prefix)))

  private[smv] def readPersistedMetadata(prefix: String = ""): Try[SmvMetadata] =
    Try {
      val json = app.sc.textFile(moduleMetaPath(prefix)).collect.head
      SmvMetadata.fromJson(json)
    }

  private[smv] def readMetadataHistory(prefix: String = ""): Try[SmvMetadataHistory] =
    Try {
      val json = app.sc.textFile(moduleMetaHistoryPath(prefix)).collect.head
      SmvMetadataHistory.fromJson(json)
    }

  private[smv] def readPersistedEdd(prefix: String = ""): Try[DataFrame] =
    Try { app.sqlContext.read.json(moduleEddPath(prefix)) }

  /** Has the result of this data set been persisted? */
  private[smv] def isPersisted: Boolean =
    Try(new FileIOHandler(app.sparkSession, moduleCsvPath()).readSchema()).isSuccess

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
      !isPersisted
  }

  /**
   * Read EDD from disk if it exists, or create and persist it otherwise
   */
  private[smv] def getEdd(collector: SmvRunInfoCollector): String = {
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

  private[smv] def persistEdd(df: DataFrame) =
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
  private[smv] def getOrCreateMetadata(dfOpt:       Option[DataFrame],
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
  private[smv] def persistMetadata(metadata: SmvMetadata): Unit = {
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
  private[smv] def persistMetadataHistory(metadata: SmvMetadata, oldHistory: SmvMetadataHistory): Unit = {
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
  private[smv] def computeDataFrame(genEdd: Boolean,
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
      

      deleteOutputs(metadataOutputFiles)
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
      readPersistedFile() match {
        // Output was persisted, so return a DF which consists of reading from
        // that persisted result
        case Success(df) =>
          app.log.info(f"Relying on cached result ${moduleCsvPath()} for ${fqn}")
          df
        // Output was not persisted (or something else went wrong), run module, persist output,
        // and return DFn which consists of reading from that persisted result
        case _ =>
          // Acquire lock on output CSV to ensure write is atomic
          SmvLock.withLock(lockfilePath()) {
            // Another process may have persisted the data while we
            // waited for the lock. So we read again before computing.
            readPersistedFile() match {
              case Success(df) =>
                app.log.info(f"Relying on cached result ${moduleCsvPath()} for ${fqn} found after lock acquired")
                df
              case _ =>
                app.log.info(f"No cached result found for ${fqn}. Caching result at ${moduleCsvPath()}")
                val df = dqmValidator.attachTasks(doRun(dqmValidator, collector, quickRun))
                // Delete outputs in case data was partially written previously
                deleteOutputs(versionedOutputFiles)
                persist(df)

                runDqmAndMeta(df, true) // has already had action (from persist)

                // Generate and persist edd based on result of reading results from disk. Avoids
                // a possibly expensive action on the result from before persisting.
                if(genEdd)
                  persistEdd(df)
                readPersistedFile().get
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
  private[smv] def publishPathNoExt(version: String) = s"${app.smvConfig.publishDir}/${version}/${fqn}"

  /** path of published data for a given version. */
  private[smv] def publishCsvPath(version: String) = publishPathNoExt(version) + ".csv"

  /** path of published metadata for a given version */
  private[smv] def publishMetaPath(version: String) = publishPathNoExt(version) + ".meta"

  /** path of published metadata history for a given version */
  private[smv] def publishHistoryPath(version: String) = publishPathNoExt(version) + ".hist"

  /**
   * Publish the current module data to the publish directory.
   * PRECONDITION: user must have specified the --publish command line option (that is where we get the version)
   */
  private[smv] def publish(collector: SmvRunInfoCollector) = {
    val df      = rdd(collector=collector)
    val version = app.smvConfig.cmdLine.publish()
    val handler = new FileIOHandler(app.sparkSession, publishCsvPath(version))
    //Same as in persist, publish null string as a special value with assumption that it's not
    //a valid data value
    handler.saveAsCsvWithSchema(df, strNullValue = "_SmvStrNull_")
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
 * Maps SmvDataSet to DataFrame by FQN. This is the type of the parameter expected
 * by SmvModule's run method.
 *
 * Subclasses `Function1[SmvDataSet, DataFrame]` so it can be used the
 * same way as before, when `runParams` was type-aliased to
 * `Map[SmvDataSet, DataFrame]`
 */
class RunParams(ds2df: Map[SmvDataSet, DataFrame]) extends (SmvDataSet => DataFrame) {
  val urn2df                         = ds2df.map { case (ds, df) => (ds.urn, df) }.toMap
  override def apply(ds: SmvDataSet) = urn2df(ds.urn)
  def size                           = ds2df.size
}

/**
 * base module class.  All SMV modules need to extend this class and provide their
 * description and dependency requirements (what does it depend on).
 * The module run method will be provided the result of all dependent inputs and the
 * result of the run is the result of this module.  All modules that depend on this module
 * will be provided the DataFrame result from the run method of this module.
 * Note: the module should *not* persist any RDD itself.
 */
abstract class SmvModule(val description: String) extends SmvDataSet {

  /**
   * flag if this module is ephemeral or short lived so that it will not be persisted when a graph is executed.
   * This is quite handy for "filter" or "map" type modules so that we don't force an extra I/O step when it
   * is not needed.  By default all modules are persisted unless the flag is overriden to true.
   * Note: the module will still be persisted if it was specifically selected to run by the user.
   */
  override def isEphemeral = false

  override def dsType() = "Module"

  type runParams = RunParams
  def run(inputs: runParams): DataFrame

  /** perform the actual run of this module to get the generated SRDD result. */
  override private[smv] def doRun(dqmValidator: DQMValidator, collector: SmvRunInfoCollector, quickRun: Boolean): DataFrame = {
    val paramMap: Map[SmvDataSet, DataFrame] =
      (resolvedRequiresDS map (dep => (dep, dep.rdd(collector=collector, quickRun=quickRun)))).toMap
    run(new runParams(paramMap))
  }

  /**
   * Create a snapshot in the current module at some result DataFrame.
   * This is useful for debugging a long SmvModule by creating snapshots along the way.
   * {{{
   * object MyMod extends SmvModule("...") {
   *   override def requiresDS = Seq(...)
   *   override def run(...) = {
   *      val s1 = ...
   *      snapshot(s1, "s1")
   *      val s2 = f(s1)
   *      snapshot(s2, "s2")
   *      ...
   *   }
   * }}}
   */
  def snapshot(df: DataFrame, prefix: String): DataFrame = {
    persist(df, prefix)
    readPersistedFile(prefix).get
  }

}

/**
 * Link to an output module in another stage.
 * Because modules in a given stage can not access modules in another stage, this class
 * enables the user to link an output module from one stage as an input into current stage.
 * {{{
 *   package stage2.input
 *
 *   object Account1Link extends SmvModuleLink(stage1.Accounts)
 * }}}
 * Similar to File/Module, a `dqm()` method can also be overriden in the link
 */
class SmvModuleLink(val outputModule: SmvOutput)
    extends SmvModule(s"Link to ${outputModule.asInstanceOf[SmvDataSet].fqn}") {

  private[smv] val smvModule = outputModule.asInstanceOf[SmvDataSet]

  override def fqn = throw new SmvRuntimeException("SmvModuleLink fqn should never be called")
  override def urn = LinkURN(smvModule.fqn)

  /** Returns the path for the module's csv output */
  override def moduleCsvPath(prefix: String = ""): String =
    throw new SmvRuntimeException("SmvModuleLink's moduleCsvPath should never be called")

  /** Returns the path for the module's schema file */
  private[smv] override def moduleSchemaPath(prefix: String = ""): String =
    throw new SmvRuntimeException("SmvModuleLink's moduleSchemaPath should never be called")

  /** Returns the path for the module's edd report output */
  private[smv] override def moduleEddPath(prefix: String = ""): String =
    throw new SmvRuntimeException("SmvModuleLink's moduleEddPath should never be called")

  /** Returns the path for the module's reject report output */
  private[smv] override def moduleValidPath(prefix: String = ""): String =
    throw new SmvRuntimeException("SmvModuleLink's moduleValidPath should never be called")

  /**
   * Get the path of the metadata for the output csv this link will read from
   * If using published data, get the target's published metadata path. Otherwise,
   * use the target's peristed metadata path.
   */
  private[smv] override def moduleMetaPath(prefix: String = ""): String = smvModule.moduleMetaPath()

  override lazy val ancestors = smvModule.ancestors

  /**
   *  No need to check isEphemeral any more
   *  SmvOutput will be published anyhow regardless of ephemeral or not
   **/
  // require(! smvModule.isEphemeral)
  // TODO: add check that the link is to an object in a different stage!!!

  private[smv] val isFollowLink = true

  override val isEphemeral = true

  override def dsType() = "Link"

  /**
   * override the module run/requiresDS methods to be a no-op as it will never be called (we overwrite doRun as well.)
   */
  override def requiresDS()           = Seq.empty[SmvDataSet]
  override def run(inputs: runParams) = null

  /**
   * Resolve the target SmvModule and wrap it in a new SmvModuleLink
   */
  override def resolve(resolver: DataSetResolver): SmvDataSet =
    new SmvModuleLink(resolver.resolveDataSet(smvModule).asInstanceOf[SmvOutput])

  /**
   * If the depended smvModule has a published version, SmvModuleLink's datasetHash
   * depends on the version string and the target's FQN (even with versioned data
   * the hash should change if the target changes). Otherwise, depends on the
   * smvModule's hashOfHash
   **/
  override def instanceValHash() = {
    val dependedHash = smvModule.hashOfHash

    (dependedHash).toInt
  }

  override def sourceCodeHash() = 0

  /**
   * SmvModuleLinks should not cache or validate their data
   */
  override def computeDataFrame(genEdd: Boolean, collector: SmvRunInfoCollector, quickRun: Boolean) =
    throw new SmvRuntimeException("SmvModuleLink computeDataFrame should never be called")
  override private[smv] def doRun(dqmValidator: DQMValidator, collector: SmvRunInfoCollector, quickRun: Boolean) =
    throw new SmvRuntimeException("SmvModuleLink doRun should never be called")

  /**
   * "Running" a link requires that we read the published output from the upstream `DataSet`.
   * When publish version is specified, it will try to read from the published dir. Otherwise
   * it will either "follow-the-link", which means resolve the modules the linked DS depends on
   * and run the DS, or "not-follow-the-link", which will try to read from the persisted data dir
   * and fail if not found.
   */
  override def rdd(forceRun: Boolean = false,
                   genEdd: Boolean = false,
                   collector: SmvRunInfoCollector,
                   quickRun: Boolean = false): DataFrame = {
    // forceRun argument is ignored (SmvModuleLink is rerun anyway)
    if (isFollowLink) {
      smvModule.rdd(collector=collector, quickRun=quickRun)
    } else {
      smvModule
        smvModule.readPersistedFile().toOption
        .getOrElse(
          throw new IllegalStateException(s"can not find published or persisted ${description}"))
    }
  }
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
    resolvedRequiresDS = urns map (urn => resolver.loadDataSet(URN(urn)).head)
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
