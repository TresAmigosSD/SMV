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

import org.apache.spark.sql.DataFrame

import dqm.{DQMValidator, SmvDQM, TerminateParserLogger, FailParserCountPolicy}

import scala.collection.JavaConversions._
import scala.util.Try

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

  lazy val app: SmvApp            = SmvApp.app
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
  def datasetHash: Int = datasetCRC.toInt

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
  def dqm(): SmvDQM  = SmvDQM()
  def getDqm: SmvDQM = dqm()

  /**
   * createDsDqm could be overridden by smv internal SmvDataSet's sub-classes
   */
  private[smv] def createDsDqm() = dqm()

  /**
   * returns the DataFrame from this dataset (file/module).
   * The value is cached so this function can be called repeatedly. The cache is
   * external to SmvDataSet so that it we will not recalculate the DF even after
   * dynamically loading the same SmvDataSet.
   * Note: the RDD graph is cached and NOT the data (i.e. rdd.cache is NOT called here)
   */
  def rdd() = {
    if (!app.dfCache.contains(versionedFqn)) {
      app.dfCache = app.dfCache + (versionedFqn -> computeRDD)
    }
    app.dfCache(versionedFqn)
  }

  private def verHex = f"${hashOfHash}%08x"
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

  /** perform the actual run of this module to get the generated SRDD result. */
  private[smv] def doRun(dsDqm: DQMValidator): DataFrame

  /**
   * delete the output(s) associated with this module (csv file and schema).
   * TODO: replace with df.write.mode(Overwrite) once we move to spark 1.4
   */
  private[smv] def deleteOutputs() = {
    val csvPath    = moduleCsvPath()
    val eddPath    = moduleEddPath()
    val schemaPath = moduleSchemaPath()
    val rejectPath = moduleValidPath()
    SmvHDFS.deleteFile(csvPath)
    SmvHDFS.deleteFile(schemaPath)
    SmvHDFS.deleteFile(eddPath)
    SmvHDFS.deleteFile(rejectPath)
  }

  /**
   * Returns current valid outputs produced by this module.
   */
  private[smv] def currentModuleOutputFiles(): Seq[String] = {
    Seq(moduleCsvPath(), moduleSchemaPath(), moduleEddPath(), moduleValidPath())
  }

  private[smv] def persist(rdd: DataFrame, prefix: String = "") =
    util.DataSet.persist(app.sparkSession, rdd, moduleCsvPath(prefix), app.genEdd)

  private[smv] def readPersistedFile(prefix: String = ""): Try[DataFrame] =
    Try(util.DataSet.readFile(app.sparkSession, moduleCsvPath(prefix)))

  private[smv] def computeRDD: DataFrame = {
    val dsDqm     = new DQMValidator(createDsDqm())
    val validator = new ValidationSet(Seq(dsDqm), isPersistValidateResult)

    if (isEphemeral) {
      val df = dsDqm.attachTasks(doRun(dsDqm))
      validator.validate(df, false, moduleValidPath()) // no action before this point
      df
    } else {
      readPersistedFile().recoverWith {
        case e =>
          val df = dsDqm.attachTasks(doRun(dsDqm))
          // Delete outputs in case data was partially written previously
          deleteOutputs
          persist(df)
          validator.validate(df, true, moduleValidPath()) // has already had action (from persist)
          readPersistedFile()
      }.get
    }
  }

  /** path of published data for a given version. */
  private def publishPath(version: String) = s"${app.smvConfig.publishDir}/${version}/${fqn}.csv"

  /**
   * Publish the current module data to the publish directory.
   * PRECONDITION: user must have specified the --publish command line option (that is where we get the version)
   */
  private[smv] def publish() = {
    val df      = rdd()
    val version = app.smvConfig.cmdLine.publish()
    val handler = new FileIOHandler(app.sparkSession, publishPath(version))
    //Same as in persist, publish null string as a special value with assumption that it's not
    //a valid data value
    handler.saveAsCsvWithSchema(df, strNullValue = "_SmvStrNull_")

    /* publish should also calculate edd if generarte Edd flag was turned on */
    if (app.genEdd)
      df.edd.persistBesideData(publishPath(version))
  }

  private[smv] lazy val parentStage: Option[String] = app.dsm.stageForUrn(urn)
  private[smv] def stageVersion()                   = parentStage flatMap { app.smvConfig.stageVersions.get(_) }

  /**
   * Read the published data of this module if the parent stage has specified a version.
   * @return Some(DataFrame) if the stage has a version specified, None otherwise.
   */
  private[smv] def readPublishedData(): Option[DataFrame] = {
    stageVersion.map { v =>
      val handler = new FileIOHandler(app.sparkSession, publishPath(v))
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
case class SmvHiveTable(override val tableName: String, val userQuery: String = null)
    extends SmvInputDataSet {
  override def description() = s"Hive Table: @${tableName}"

  val query = {
    if (userQuery == null)
      "select * from " + tableName
    else
      userQuery
  }

  override private[smv] def doRun(dsDqm: DQMValidator): DataFrame = {
    val df = app.sparkSession.sql(query)
    run(df)
  }
}

/**
 * Both SmvFile and SmvCsvStringData shared the parser validation part, extract the
 * common part to the new ABC: SmvDSWithParser
 */
trait SmvDSWithParser extends SmvDataSet {
  val forceParserCheck   = true
  val failAtParsingError = true

  override def createDsDqm() =
    if (failAtParsingError) dqm().add(FailParserCountPolicy(1)).addAction()
    else if (forceParserCheck) dqm().addAction()
    else dqm()
}

abstract class SmvFile extends SmvInputDataSet {
  val path: String
  val schemaPath: String     = null
  override def description() = s"Input file: @${path}"

  private[smv] def isFullPath: Boolean = false

  protected def findFullPath(_path: String) = {
    if (isFullPath || ("""^[\.\/]""".r).findFirstIn(_path) != None) _path
    else if (_path.startsWith("input/")) s"${app.smvConfig.dataDir}/${_path}"
    else s"${app.smvConfig.inputDir}/${_path}"
  }

  /* Historically we specify path in SmvFile respect to dataDir
   * instead of inputDir. However by convention we always put data
   * files in /data/input/ dir, so all the path strings in the projects
   * started with "input/". To transfer to use inputDir, we will still
   * prepend dataDir if the path string start with "input/"
   */
  private[smv] def fullPath = findFullPath(path)

  private[smv] def fullSchemaPath = {
    if (schemaPath == null) None
    else Option(findFullPath(schemaPath))
  }

  /* For SmvFile, the datasetHash should be based on
   *  - raw class code crc
   *  - input csv file path
   *  - input csv file modified time
   *  - input schema file path
   *  - input schema file modified time
   */
  override def datasetHash() = {
    val fileName = fullPath
    val mTime    = SmvHDFS.modificationTime(fileName)

    val schemaPath  = fullSchemaPath.getOrElse(SmvSchema.dataPathToSchemaPath(fullPath))
    val schemaMTime = SmvHDFS.modificationTime(schemaPath)

    val crc = new java.util.zip.CRC32
    crc.update((fileName + schemaPath).toCharArray.map(_.toByte))
    (crc.getValue + mTime + schemaMTime + datasetCRC).toInt
  }
}

/**
 * Represents a raw input file with a given file path (can be local or hdfs) and CSV attributes.
 */
case class SmvCsvFile(
    override val path: String,
    csvAttributes: CsvAttributes = null,
    override val schemaPath: String = null,
    override val isFullPath: Boolean = false
) extends SmvFile
    with SmvDSWithParser {

  override private[smv] def doRun(dsDqm: DQMValidator): DataFrame = {
    val parserValidator =
      if (dsDqm == null) TerminateParserLogger else dsDqm.createParserValidator()
    // TODO: this should use inputDir instead of dataDir
    val handler = new FileIOHandler(app.sparkSession, fullPath, fullSchemaPath, parserValidator)
    val df      = handler.csvFileWithSchema(csvAttributes)
    run(df)
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
    override val schemaPath: String = null
) extends SmvFile
    with SmvDSWithParser {

  override val path = dir

  override def fullSchemaPath = {
    if (schemaPath == null) Option(SmvSchema.dataPathToSchemaPath(fullPath))
    else Option(findFullPath(schemaPath))
  }

  override private[smv] def doRun(dsDqm: DQMValidator): DataFrame = {
    val parserValidator =
      if (dsDqm == null) TerminateParserLogger else dsDqm.createParserValidator()

    val filesInDir = SmvHDFS.dirList(fullPath).map { n =>
      s"${fullPath}/${n}"
    }

    if (filesInDir.isEmpty)
      throw new SmvRuntimeException(s"There are no data files in ${fullPath}")

    val df = filesInDir
      .map { s =>
        val handler = new FileIOHandler(app.sparkSession, s, fullSchemaPath, parserValidator)
        handler.csvFileWithSchema(csvAttributes)
      }
      .reduce(_ union _)

    run(df)
  }
}

case class SmvFrlFile(
    override val path: String,
    override val schemaPath: String = null,
    override val isFullPath: Boolean = false
) extends SmvFile
    with SmvDSWithParser {

  override private[smv] def doRun(dsDqm: DQMValidator): DataFrame = {
    val parserValidator =
      if (dsDqm == null) TerminateParserLogger else dsDqm.createParserValidator()
    // TODO: this should use inputDir instead of dataDir
    val handler = new FileIOHandler(app.sparkSession, fullPath, fullSchemaPath, parserValidator)
    val df      = handler.frlFileWithSchema()
    run(df)
  }
}

/**
 * Maps SmvDataSet to DataFrame by FQN. This is the type of the parameter expected
 * by SmvModule's run method.
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
  override private[smv] def doRun(dsDqm: DQMValidator): DataFrame = {
    val paramMap: Map[SmvDataSet, DataFrame] =
      (resolvedRequiresDS map (dep => (dep, dep.rdd))).toMap
    run(new runParams(paramMap))
  }

  /** Use Bytecode analysis to figure out dependency and check against
   *  resolvedRequiresDS and requiresAnc. Could consider to totaly drop resolvedRequiresDS and
   *  requiresAnc, and always use ASM to derive the dependency
   **/
  private def checkDependency(): Unit = {
    val dep = DataSetDependency(this.getClass.getName)
    dep.dependsAnc
      .map { s =>
        (s, SmvReflection.objectNameToInstance[SmvAncillary](s))
      }
      .filterNot { case (s, a) => requiresAnc().contains(a) }
      .foreach {
        case (s, a) =>
          throw new SmvRuntimeException(s"SmvAncillary ${s} need to be specified in requiresAnc")
      }
    dep.dependsDS
      .map { s =>
        (s, SmvReflection.objectNameToInstance[SmvDataSet](s))
      }
      .filterNot { case (s, a) => resolvedRequiresDS.contains(a) }
      .foreach {
        case (s, a) =>
          throw new SmvRuntimeException(
            s"SmvDataSet ${s} need to be specified in requiresDS, ${a}")
      }
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

  override def urn = LinkURN(smvModule.fqn)

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
   * depends on the version string. Otherwise, depends on the smvModule's hashOfHash
   **/
  override def datasetHash() = {
    val dependedHash = smvModule.stageVersion
      .map { v =>
        val crc = new java.util.zip.CRC32
        crc.update(v.toCharArray.map(_.toByte))
        (crc.getValue).toInt
      }
      .getOrElse(smvModule.hashOfHash)

    (dependedHash + datasetCRC).toInt
  }

  /**
   * SmvModuleLinks should not cache or validate their data
   */
  override def computeRDD = throw new SmvRuntimeException("SmvModuleLink computeRDD should never be called")
  override private[smv] def doRun(dsDqm: DQMValidator) = throw new SmvRuntimeException("SmvModuleLink doRun should never be called")

  /**
   * "Running" a link requires that we read the published output from the upstream `DataSet`.
   * When publish version is specified, it will try to read from the published dir. Otherwise
   * it will either "follow-the-link", which means resolve the modules the linked DS depends on
   * and run the DS, or "not-follow-the-link", which will try to read from the persisted data dir
   * and fail if not found.
   */
  override def rdd: DataFrame = {
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
class SmvExtModulePython(target: ISmvModule) extends SmvDataSet {
  override val description = s"SmvPyModule ${target.fqn}"
  override val fqn         = target.fqn
  override def tableName   = target.tableName()
  override def isEphemeral = target.isEphemeral()
  override def dsType      = target.dsType()
  override def requiresDS =
    throw new SmvRuntimeException("SmvExtModulePython requiresDS should never be called")
  override def resolve(resolver: DataSetResolver): SmvDataSet = {
    resolvedRequiresDS = target.dependencies map (urn => resolver.loadDataSet(URN(urn)).head)
    this
  }
  override private[smv] def doRun(dsDqm: DQMValidator): DataFrame =
    target.getDataFrame(new DQMValidator(createDsDqm),
                        resolvedRequiresDS
                          .map { ds =>
                            (ds.urn.toString, ds.rdd)
                          }
                          .toMap[String, DataFrame])
  override def datasetHash = target.datasetHash()
  override def createDsDqm = target.getDqm()
}

/**
 * Factory for SmvExtModulePython. Creates an SmvExtModulePython with SmvOuptut
 * if the Python dataset is SmvPyOutput
 */
object SmvExtModulePython {
  def apply(target: ISmvModule): SmvExtModulePython = {
    if (target.isOutput)
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
case class SmvCsvStringData(
    schemaStr: String,
    data: String,
    override val isPersistValidateResult: Boolean = false
) extends SmvInputDataSet
    with SmvDSWithParser {

  override def description() = s"Dummy module to create DF from strings"

  override def datasetHash() = {
    val crc = new java.util.zip.CRC32
    crc.update((schemaStr + data).toCharArray.map(_.toByte))
    (crc.getValue + datasetCRC).toInt
  }

  override def doRun(dsDqm: DQMValidator): DataFrame = {
    val schema    = SmvSchema.fromString(schemaStr)
    val dataArray = if (null == data) Array.empty[String] else data.split(";").map(_.trim)

    val parserValidator =
      if (dsDqm == null) TerminateParserLogger else dsDqm.createParserValidator()
    val handler = new FileIOHandler(app.sparkSession, null, None, parserValidator)
    handler.csvStringRDDToDF(app.sc.makeRDD(dataArray), schema, schema.extractCsvAttributes())
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
