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

import dqm.{DQMValidator, ParserLogger, SmvDQM, TerminateParserLogger, FailParserCountPolicy}

import scala.collection.JavaConversions._
import scala.util.Try

import edd._

import org.joda.time._, format._

/** A module's file name part is stackable, e.g. with Using[SmvRunConfig] */
trait FilenamePart {
  def fnpart: String
}

/**
 * Dependency management unit within the SMV application framework.  Execution order within
 * the SMV application framework is derived from dependency between SmvDataSet instances.
 * Instances of this class can either be a file or a module. In either case, there would
 * be a single result DataFrame.
 */
abstract class SmvDataSet extends FilenamePart {

  def app: SmvApp                 = SmvApp.app
  private var rddCache: DataFrame = null

  /**
   * The FQN of an SmvDataSet is its classname for Scala implementations.
   *
   * Scala proxies for implementations in other languages must
   * override this to name the proxied FQN.
   */
  def fqn: String       = this.getClass().getName().filterNot(_ == '$')
  def urn: URN          = ModURN(fqn)
  override def toString = urn.toString

  /** Names the persisted file for the result of this SmvDataSet */
  override def fnpart = fqn

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

  def requiresAnc(): Seq[SmvAncillary] = Seq.empty

  /** TODO: remove this method as checkDependency replaced this function */
  def getAncillary[T <: SmvAncillary](anc: T) = {
    if (requiresAnc.contains(anc)) anc
    else throw new SmvRuntimeException(s"SmvAncillary: ${anc} is not in requiresAnc")
  }

  /** user tagged code "version".  Derived classes should update the value when code or data */
  def version(): Int = 0

  /** full name of hive output table if this module is published to hive. */
  def tableName: String = throw new IllegalStateException("tableName not specified for ${fqn}")

  /** Objects defined in Spark Shell has class name start with $ **/
  val isObjectInShell: Boolean = this.getClass.getName matches """\$.*"""

  /**
   * SmvDataSet code (not data) CRC. Always return 0 for objects created in spark shell
   */
  private[smv] lazy val datasetCRC = {
    if (isObjectInShell)
      0l
    else
      ClassCRC(this)
  }

  /** Hash computed from the dataset, could be overridden to include things other than CRC */
  def datasetHash(): Int = instanceValHash + sourceCodeHash
  /** Hash computed based on instance values of the dataset, such as the timestamp of an input file **/
  def instanceValHash(): Int = 0
  /** Hash computed based on the source code of the dataset's class **/
  def sourceCodeHash(): Int = datasetCRC.toInt

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
  def exportToHive = {
    val dataframe = rdd()
    // register the dataframe as a temp table.  Will be overwritten on next register.
    dataframe.registerTempTable("dftable")

    // if user provided a publish hive sql command, run it instead of default
    // table creation from data frame result.
    if (publishHiveSql.isDefined) {
      publishHiveSql.get.split(";").map {stmt => app.sqlContext.sql(stmt.trim)}
    } else {
      app.sqlContext.sql(s"drop table if exists ${tableName}")
      app.sqlContext.sql(s"create table ${tableName} as select * from dftable")
    }
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
    userDQM.add(new DQMMetadataPolicy(this))

  /**
   * returns the DataFrame from this dataset (file/module).
   * The value is cached so this function can be called repeatedly. The cache is
   * external to SmvDataSet so that it we will not recalculate the DF even after
   * dynamically loading the same SmvDataSet. If force argument is true, the we
   * skip the cache.
   * Note: the RDD graph is cached and NOT the data (i.e. rdd.cache is NOT called here)
   */
  def rdd(forceRun: Boolean = false, genEdd: Boolean = app.genEdd) = {
    if (forceRun || !app.dfCache.contains(versionedFqn)) {
      app.dfCache = app.dfCache + (versionedFqn -> computeRDD(genEdd))
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

  private[smv] def moduleMetaHistoryPath(prefix: String = ""): String =
    s"""${app.smvConfig.outputDir}/${prefix}${fqn}.meta"""

  /** perform the actual run of this module to get the generated SRDD result. */
  private[smv] def doRun(dqmValidator: DQMValidator): DataFrame

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
   * Files relateto metadata
   */
  private[smv] def metadataOutputFiles(): Seq[String] =
    Seq(moduleMetaPath(), moduleMetaHistoryPath())

  /**
   * Returns current valid outputs produced by this module.
   */
  private[smv] def allOutputFiles(): Seq[String] = {
    Seq(moduleCsvPath(), moduleSchemaPath(), moduleEddPath(), moduleValidPath(), moduleMetaPath(), moduleMetaHistoryPath())
  }

  /**
   * Returns current versioned valid outputs produced by this module. Excludes metadata history
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
    new FileIOHandler(app.sqlContext, path).csvFileWithSchema(attr)


  def persist(dataframe: DataFrame,
              prefix: String = ""): Unit = {
    val path = moduleCsvPath(prefix)
    val fmt = DateTimeFormat.forPattern("HH:mm:ss")

    val counter = app.sqlContext.sparkContext.accumulator(0l)
    val before  = DateTime.now()
    println(s"${fmt.print(before)} PERSISTING: ${path}")

    val df      = dataframe.smvPipeCount(counter)
    val handler = new FileIOHandler(app.sqlContext, path)

    //Always persist null string as a special value with assumption that it's not
    //a valid data value
    handler.saveAsCsvWithSchema(df, strNullValue = "_SmvStrNull_")

    val after   = DateTime.now()
    val runTime = PeriodFormat.getDefault().print(new Period(before, after))
    val n       = counter.value

    println(s"${fmt.print(after)} RunTime: ${runTime}, N: ${n}")
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
    Try(new FileIOHandler(app.sqlContext, moduleCsvPath()).readSchema()).isSuccess

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
  private[smv] def getEdd(): String = {
    // DON'T automatically persist edd. Edd is explicitly persisted on the next
    // line. This is the simplest way to prevent EDD from being persisted twice.
    val df = rdd(forceRun = false, genEdd = false)

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
   */
  def userMetadata(df: DataFrame): SmvMetadata =
    new SmvMetadata()

  /**
   * Get the most detailed metadata available without running this module. If
   * the modules has been run and hasn't been changed, this will be all the metadata
   * that was persisted. If the module hasn't been run since it was changed, this
   * will be a less detailed report.
   */
  private[smv] def getMetadata(): SmvMetadata =
    readPersistedMetadata().getOrElse(createMetadata(None))

  private[smv] def getMetadataHistory(): SmvMetadataHistory =
    readMetadataHistory().getOrElse(SmvMetadataHistory.empty)

  /**
   * Create SmvMetadata for this SmvDataset. SmvMetadata will be more detailed if
   * a DataFrame is provided
   */
  private[smv] def createMetadata(dfOpt: Option[DataFrame]): SmvMetadata = {
    val metadata = dfOpt match {
      case Some(df) => userMetadata(df)
      case _        => new SmvMetadata()
    }
    metadata.addFQN(fqn)
    metadata.addDependencyMetadata(resolvedRequiresDS)
    dfOpt foreach (metadata.addSchemaMetadata(_))
    timestamp foreach (metadata.addTimestamp(_))
    metadata
  }

  private[smv] def persistMetadata(metadata: SmvMetadata): Unit =
    metadata.saveToFile(app.sc, moduleMetaPath())

  private[smv] def metadataHistorySize(): Integer = 5

  private[smv] def persistMetadataHistory(metadata: SmvMetadata, metadataHistory: SmvMetadataHistory): Unit =
    metadataHistory
      .update(metadata, metadataHistorySize)
      .saveToFile(app.sc, moduleMetaHistoryPath())

  /**
   * Override to validate module results based on current and historic metadata.
   * If false, DQM will fail. Defaults to true (no-op).
   */
  def validateMetadata(metadata: SmvMetadata, history: Seq[SmvMetadata]): Boolean =
    true

  private[smv] def computeRDD(genEdd: Boolean): DataFrame = {
    val dqmValidator  = new DQMValidator(dqmWithTypeSpecificPolicy(dqm()), isPersistValidateResult)

    if (isEphemeral) {
      val df = dqmValidator.attachTasks(doRun(dqmValidator))
      dqmValidator.validate(df, false, moduleValidPath()) // no action before this point

      val metadata = createMetadata(Some(df))
      // must read metadata from file (if it exists) before deleting outputs
      val metadataHistory = getMetadataHistory
      deleteOutputs(metadataOutputFiles)
      persistMetadata(metadata)
      persistMetadataHistory(metadata, metadataHistory)

      df
    } else {
      readPersistedFile().recoverWith {
        case e =>
          SmvLock.withLock(moduleCsvPath() + ".lock") {
            // Another process may have persisted the data while we
            // waited for the lock. So we read again before computing.
            readPersistedFile().recoverWith { case x =>
              val df = dqmValidator.attachTasks(doRun(dqmValidator))
              // Delete outputs in case data was partially written previously
              deleteOutputs(versionedOutputFiles)
              persist(df)
              dqmValidator.validate(df, true, moduleValidPath()) // has already had action (from persist)

              val metadata = createMetadata(Some(df))
              // must read metadata from file (if it exists) before deleting outputs
              val metadataHistory = getMetadataHistory
              deleteOutputs(metadataOutputFiles)
              persistMetadata(metadata)
              persistMetadataHistory(metadata, metadataHistory)

              // Generate and persist edd based on result of reading results from disk. Avoids
              // a possibly expensive action on the result from before persisting.
              if(genEdd)
                persistEdd(df)
              readPersistedFile()
            }
          }
      }.get
    }
  }

  /** path to published output without file extension **/
  private[smv] def publishPathNoExt(version: String) = s"${app.smvConfig.publishDir}/${version}/${fqn}"

  /** path of published data for a given version. */
  private[smv] def publishCsvPath(version: String) = publishPathNoExt(version) + ".csv"

  /** path of published metadata for a given version */
  private[smv] def publishMetaPath(version: String) = publishPathNoExt(version) + ".meta"

  /**
   * Publish the current module data to the publish directory.
   * PRECONDITION: user must have specified the --publish command line option (that is where we get the version)
   */
  private[smv] def publish() = {
    val df      = rdd()
    val version = app.smvConfig.cmdLine.publish()
    val handler = new FileIOHandler(app.sqlContext, publishCsvPath(version))
    //Same as in persist, publish null string as a special value with assumption that it's not
    //a valid data value
    handler.saveAsCsvWithSchema(df, strNullValue = "_SmvStrNull_")
    createMetadata(Some(df)).saveToFile(app.sc, publishMetaPath(version))

    /* publish should also calculate edd if generarte Edd flag was turned on */
    if (app.genEdd)
      df.edd.persistBesideData(publishCsvPath(version))
  }

  /**
   * Publish DataFrame result using JDBC. Url will be user-specified.
   */
  private[smv] def publishThroughJDBC = {
    val df = rdd()
    val connectionProperties = new java.util.Properties()
    val url = app.smvConfig.jdbcUrl
    df.write.mode(SaveMode.Append).jdbc(url, tableName, connectionProperties)
  }

  private[smv] def parentStage: Option[String] = urn.getStage

  private[smv] def stageVersion()                   = parentStage flatMap { app.smvConfig.stageVersions.get(_) }

  /**
   * Read the published data of this module if the parent stage has specified a version.
   * @return Some(DataFrame) if the stage has a version specified, None otherwise.
   */
  private[smv] def readPublishedData(version: Option[String] = stageVersion): Option[DataFrame] = {
    version.map { v =>
      val handler = new FileIOHandler(app.sqlContext, publishCsvPath(v))
      handler.csvFileWithSchema(null)
    }
  }
}

/**
 * Abstract out the common part of input SmvDataSet
 */
private[smv] abstract class SmvInputDataSet extends SmvDataSet {
  override def requiresDS() = Seq.empty
  override val isEphemeral  = true

  override def dsType() = "Input"

  /**
   * Method to run/pre-process the input file.
   * Users can override this method to perform file level
   * ETL operations.
   */
  def run(df: DataFrame) = df
}

/**
 * SMV Dataset Wrapper around a hive table.
 */
class SmvHiveTable(override val tableName: String, val userQuery: String = null)
    extends SmvInputDataSet {
  override def description() = s"Hive Table: @${tableName}"

  val query = {
    if (userQuery == null)
      "select * from " + tableName
    else
      userQuery
  }

  override private[smv] def doRun(dqmValidator: DQMValidator): DataFrame = {
    val df = app.sqlContext.sql(query)
    run(df)
  }
}

object SmvHiveTable {
  def apply(tableName: String, userQuery: String = null): SmvHiveTable = {
    new SmvHiveTable(tableName, userQuery)
  }
}

/**
 * Wrapper for a database table accessed via JDBC
 */
class SmvJdbcTable(override val tableName: String)
  extends SmvInputDataSet {

  override def description = s"JDBC table ${tableName}"

  /**
   * Custom queries are not officially supported because the approach used here
   * is not documented or officially supported by Spark. We will essentially
   * substitute the user-query as a subquery in place of the table name, with
   * the result a query like SELECT * FROM (USER_QUERY)
   */
  def userQuery: String = null

  val tableNameOrQuery = {
    if (userQuery == null){
      tableName
    } else {
      // For Derby, subqueries must be aliased
      s"(${userQuery}) as TMP_${tableName}"
    }
  }

  override private[smv] def doRun(dqmValidator: DQMValidator): DataFrame = {
    val url = app.smvConfig.jdbcUrl
    val tableDf =
      app.sqlContext.read
        .format("jdbc")
        .option("url", url)
        .option("dbtable", tableNameOrQuery)
        .load()
    run(tableDf)
  }
}

/**
 * Both SmvFile and SmvCsvStringData shared the parser validation part, extract the
 * common part to the new ABC: SmvDSWithParser
 */
trait SmvDSWithParser extends SmvDataSet {
  val forceParserCheck   = true
  val failAtParsingError = true

  /**
   *  Add parser failure policy to any DataSets that use a parser (e.g. csv files and hive tables)
   */
  override def dqmWithTypeSpecificPolicy(userDQM: SmvDQM) = {
    val baseDqm = super.dqmWithTypeSpecificPolicy(userDQM)
    if (failAtParsingError) baseDqm.add(FailParserCountPolicy(1)).addAction()
    else if (forceParserCheck) baseDqm.addAction()
    else baseDqm
  }
}


abstract class SmvFile extends SmvInputDataSet with SmvDSWithParser {
  val path: String
  val schemaPath: String     = null
  override def description() = s"Input file: @${path}"

  private[smv] def isFullPath: Boolean = false

  protected def findFullPath(_path: String) = {
    if (isFullPath || ("""^[\.\/]""".r).findFirstIn(_path) != None) _path
    else s"${app.smvConfig.inputDir}/${_path}"
  }

  private[smv] def getHandler(csvPath: String, parserValidator: ParserLogger) =
    new FileIOHandler(app.sqlContext, csvPath, fullSchemaPath, parserValidator)

  /* Historically we specify path in SmvFile respect to dataDir
   * instead of inputDir. However by convention we always put data
   * files in /data/input/ dir, so all the path strings in the projects
   * started with "input/". To transfer to use inputDir, we will still
   * prepend dataDir if the path string start with "input/"
   */
  private[smv] def fullPath = findFullPath(path)

  /**
   * Expanded version of user-specified schema path, if it exists
   */
  private[smv] def fullSchemaPath = {
    if (schemaPath == null) None
    else Option(findFullPath(schemaPath))
  }

  /**
   * The schema that will actually be read from. If there is a user-specified
   * schema path, this will be full path of that schema path. If not, this will
   * be the default location for a schema relative to the file path.
   */
  private def finalSchemaPath =
    fullSchemaPath.getOrElse { SmvSchema.dataPathToSchemaPath(fullPath) }

  val userSchema: Option[String]

  /**
   * SmvSchema for this file. If a userSchema String is specified, the SmvSchema
   * is read from that. If not, it is read from file.
   */
  lazy val schema: SmvSchema =
    userSchema match {
      case Some(s) => SmvSchema.fromString(s)
      case None => SmvSchema.fromFile(app.sc, finalSchemaPath)
    }

  /**
   * Read contents from file (without running the `run` method) as a DataFrame.
   */
  private[smv] def readFromFile(parserLogger: ParserLogger): DataFrame

  override private[smv] def doRun(dqmValidator: DQMValidator): DataFrame = {
    val parserValidator =
      if (dqmValidator == null) TerminateParserLogger else dqmValidator.createParserValidator()
    val df      = readFromFile(parserValidator)
    run(df)
  }

  /* For SmvFile, the datasetHash should be based on
   *  - raw class code crc
   *  - input csv file path
   *  - input csv file modified time
   *  - input schema contents
   */
  override def instanceValHash() = {
    val fileName = fullPath
    val mTime    = SmvHDFS.modificationTime(fileName)

    val crc = new java.util.zip.CRC32

    crc.update(fileName.toCharArray.map(_.toByte))
    (crc.getValue + mTime + schema.schemaHash).toInt
  }
}

/**
 * Represents a single raw input file with a given file path. E.g. SmvCsvFile or SmvFrlFile
 */
abstract class SmvSingleFile extends SmvFile {
  /**
   * Given a FileIOHandler, return the DataFrame that results from reading the file
   */
  private[smv] def readSingleFile(handler: FileIOHandler): DataFrame
  private[smv] override def readFromFile(parserValidator: ParserLogger): DataFrame = {
    val handler = getHandler(fullPath, parserValidator)
    readSingleFile(handler)
  }
}


/**
 * Represents a raw input file with a given file path (can be local or hdfs) and CSV attributes.
 */
class SmvCsvFile(
    override val path: String,
    csvAttributes: CsvAttributes = null,
    override val schemaPath: String = null,
    override val isFullPath: Boolean = false,
    override val userSchema: Option[String] = None
) extends SmvSingleFile {
  def readSingleFile(handler: FileIOHandler) =
    handler.csvFileWithSchema(csvAttributes, Some(schema))
}

object SmvCsvFile {
  def apply(
      path: String,
      csvAttributes: CsvAttributes = null,
      schemaPath: String = null,
      isFullPath: Boolean = false,
      userSchema: Option[String] = None
  ): SmvCsvFile = {
    new SmvCsvFile(path, csvAttributes, schemaPath, isFullPath, userSchema)
  }
}

class SmvFrlFile(
    override val path: String,
    override val schemaPath: String = null,
    override val isFullPath: Boolean = false,
    override val userSchema: Option[String] = None
) extends SmvSingleFile {
  def readSingleFile(handler: FileIOHandler) =
    handler.frlFileWithSchema(Some(schema))
}

object SmvFrlFile {
  def apply(
    path: String,
    schemaPath: String = null,
    isFullPath: Boolean = false,
    userSchema: Option[String] = None
  ): SmvFrlFile = {
    new SmvFrlFile(path, schemaPath, isFullPath, userSchema)
  }
}

/**
 * Instead of a single input file, specify a data dir with files which has
 * the same schema and CsvAttributes.
 *
 * `SmvCsvFile` can also take dir as path parameter, but all files are considered
 * slices. In that case if none of them has headers, it's equivalent to `SmvMultiCsvFiles`.
 * However if every file has header, `SmvCsvFile` will not remove header correctly.
 **/
class SmvMultiCsvFiles(
    dir: String,
    csvAttributes: CsvAttributes = null,
    override val schemaPath: String = null,
    override val userSchema: Option[String] = None
) extends SmvFile {

  override val path = dir

  override def fullSchemaPath = {
    if (schemaPath == null) Option(SmvSchema.dataPathToSchemaPath(fullPath))
    else Option(findFullPath(schemaPath))
  }

  private[smv] override def readFromFile(parserValidator: ParserLogger): DataFrame = {
    val filesInDir = SmvHDFS.dirList(fullPath)
      .filterNot(_.startsWith(".")) // ignore all hidden files in the data dir
      .map { n =>
        s"${fullPath}/${n}"
      }

    if (filesInDir.isEmpty)
      throw new SmvRuntimeException(s"There are no data files in ${fullPath}")

    val df = filesInDir
      .map { filePath =>
        val handler =   getHandler(filePath, parserValidator)
        handler.csvFileWithSchema(csvAttributes, Some(schema))
      }
      .reduce(_ unionAll _)

    df
  }
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
  override private[smv] def doRun(dqmValidator: DQMValidator): DataFrame = {
    val paramMap: Map[SmvDataSet, DataFrame] =
      (resolvedRequiresDS map (dep => (dep, dep.rdd()))).toMap
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
  private[smv] override def moduleMetaPath(prefix: String = ""): String =
    smvModule.stageVersion match {
      case Some(v) => smvModule.publishMetaPath(v)
      case _ => smvModule.moduleMetaPath()
    }

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
    val dependedHash = smvModule.stageVersion
      .map { v =>
        val crc = new java.util.zip.CRC32
        crc.update((v + smvModule.fqn).toCharArray.map(_.toByte))
        (crc.getValue).toInt
      }
      .getOrElse(smvModule.hashOfHash)

    (dependedHash).toInt
  }

  /**
   * SmvModuleLinks should not cache or validate their data
   */
  override def computeRDD(genEdd: Boolean) =
    throw new SmvRuntimeException("SmvModuleLink computeRDD should never be called")
  override private[smv] def doRun(dqmValidator: DQMValidator) =
    throw new SmvRuntimeException("SmvModuleLink doRun should never be called")

  /**
   * "Running" a link requires that we read the published output from the upstream `DataSet`.
   * When publish version is specified, it will try to read from the published dir. Otherwise
   * it will either "follow-the-link", which means resolve the modules the linked DS depends on
   * and run the DS, or "not-follow-the-link", which will try to read from the persisted data dir
   * and fail if not found.
   */
  override def rdd(forceRun: Boolean = false, genEdd: Boolean = false): DataFrame = {
    // forceRun argument is ignored (SmvModuleLink is rerun anyway)
    if (isFollowLink) {
      smvModule.readPublishedData().getOrElse(smvModule.rdd())
    } else {
      smvModule
        .readPublishedData()
        .orElse { smvModule.readPersistedFile().toOption }
        .getOrElse(
          throw new IllegalStateException(s"can not find published or persisted ${description}"))
    }
  }
}

/**
 * Class for declaring datasets defined in another language. Resolves to an
 * instance of SmvExtModulePython.
 */
case class SmvExtModule(modFqn: String) extends SmvModule(s"External module ${modFqn}") {
  override val fqn = modFqn
  override def dsType(): String =
    throw new SmvRuntimeException("SmvExtModule dsType should never be called")
  override def requiresDS =
    throw new SmvRuntimeException("SmvExtModule requiresDS should never be called")
  override def resolve(resolver: DataSetResolver): SmvDataSet =
    resolver.loadDataSet(urn).head.asInstanceOf[SmvExtModulePython]
  override def run(i: RunParams) =
    throw new SmvRuntimeException("SmvExtModule run should never be called")
}

/**
 * Declarative class for links to datasets defined in another language. Resolves
 * to a link to an SmvExtModulePython.
 */
case class SmvExtModuleLink(modFqn: String)
    extends SmvModuleLink(new SmvExtModule(modFqn) with SmvOutput)

/**
 * Concrete SmvDataSet representation of modules defined in Python. Created
 * exclusively by DataSetRepoPython. Wraps an ISmvModule.
 */
class SmvExtModulePython(target: ISmvModule) extends SmvDataSet with python.InterfacesWithPy4J {
  override val fqn            = getPy4JResult(target.getFqn)
  override val description    = s"SmvModule ${fqn}"
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

  override private[smv] def doRun(dqmValidator: DQMValidator): DataFrame = {
    val urn2df = resolvedRequiresDS.map { ds =>(ds.urn.toString, ds.rdd())}.toMap[String, DataFrame]
    val response =  target.getDoRun(dqmValidator, urn2df)
    return getPy4JResult(response)
  }

  override def instanceValHash =
    getPy4JResult(target.getInstanceValHash)

  override def sourceCodeHash =
    getPy4JResult(target.getSourceCodeHash)

  override def dqm =
    getPy4JResult(target.getDqmWithTypeSpecificPolicy)
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
 * a built-in SmvModule from schema string and data string
 *
 * E.g.
 * {{{
 * SmvCsvStringData("a:String;b:Double;c:String", "aa,1.0,cc;aa2,3.5,CC")
 * }}}
 *
 **/
class SmvCsvStringData(
    schemaStr: String,
    data: String,
    override val isPersistValidateResult: Boolean = false
) extends SmvInputDataSet
    with SmvDSWithParser {

  override def description() = s"Dummy module to create DF from strings"

  override def instanceValHash() = {
    val crc = new java.util.zip.CRC32
    crc.update((schemaStr + data).toCharArray.map(_.toByte))
    (crc.getValue).toInt
  }

  override def doRun(dqmValidator: DQMValidator): DataFrame = {
    val schema    = SmvSchema.fromString(schemaStr)
    val dataArray = if (null == data) Array.empty[String] else data.split(";").map(_.trim)

    val parserValidator =
      if (dqmValidator == null) TerminateParserLogger else dqmValidator.createParserValidator()
    val handler = new FileIOHandler(app.sqlContext, null, None, parserValidator)
    handler.csvStringRDDToDF(app.sc.makeRDD(dataArray), schema, schema.extractCsvAttributes())
  }
}

object SmvCsvStringData {
  def apply(
      schemaStr: String,
      data: String,
      isPersistValidateResult: Boolean = false
  ): SmvCsvStringData = {
    new SmvCsvStringData(schemaStr, data, isPersistValidateResult)
  }
}

/**
 * A marker trait that indicates that a SmvDataSet/SmvModule decorated with this trait is an output DataSet/module.
 */
trait SmvOutput { this: SmvDataSet =>
  override def dsType(): String = "Output"
}

/** Base marker trait for run configuration objects */
trait SmvRunConfig

/**
 * SmvDataSet that can be configured to return different DataFrames.
 */
trait Using[+T <: SmvRunConfig] extends FilenamePart { self: SmvDataSet =>

  lazy val confObjName = self.app.smvConfig.runConfObj

  /** The actual run configuration object */
  lazy val runConfig: T = {
    require(
      confObjName.isDefined,
      s"Expected a run configuration object provided with ${SmvConfig.RunConfObjKey} but found none")

    import scala.reflect.runtime.{universe => ru}
    val mir = ru.runtimeMirror(getClass.getClassLoader)

    val sym    = mir.staticModule(confObjName.get)
    val module = mir.reflectModule(sym)
    module.instance.asInstanceOf[T]
  }

  // Configurable SmvDataSet has the configuration object appended to its name
  abstract override def fnpart = {
    val confObjStr = confObjName.get
    super.fnpart + '-' + confObjStr.substring(1 + confObjStr.lastIndexOf('.'))
  }
}
