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

import scala.util.Try
import org.joda.time._
import org.joda.time.format._
import dqm._

/**
 * Dependency management unit within the SMV application framework.  Execution order within
 * the SMV application framework is derived from dependency between SmvDataSet instances.
 * Instances of this class can either be a file or a module. In either case, there would
 * be a single result DataFrame.
 */
abstract class SmvDataSet {

  lazy val app: SmvApp = SmvApp.app
  private var rddCache: DataFrame = null

  def name() = this.getClass().getName().filterNot(_=='$')
  def description(): String

  /** modules must override to provide set of datasets they depend on. */
  def requiresDS() : Seq[SmvDataSet]

  def requiresAnc() : Seq[SmvAncillary] = Seq.empty

  /** TODO: remove this method as checkDependency replaced this function */
  def getAncillary[T <: SmvAncillary](anc: T) = {
    if (requiresAnc.contains(anc)) anc
    else throw new IllegalArgumentException(s"SmvAncillary: ${anc} is not in requiresAnc")
  }

  /** user tagged code "version".  Derived classes should update the value when code or data */
  def version() : Int = 0


  /** Objects defined in Spark Shell has class name start with $ **/
  val isObjectInShell: Boolean = this.getClass.getName matches """\$.*"""

  /**
   * SmvDataSet code (not data) CRC. Always return 0 for objects created in spark shell
   */
  private[smv] val datasetCRC = {
    if (isObjectInShell)
      0l
    else
      ClassCRC(this)
  }

  /** Hash computed from the dataset, could be overridden to include things other than CRC */
  def datasetHash(): Int = datasetCRC.toInt

  /**
   * Determine the hash of this module and the hash of hash (HOH) of all the modules it depends on.
   * This way, if this module or any of the modules it depends on changes, the HOH should change.
   * The "version" of the current dataset is also used in the computation to allow user to force
   * a change in the hash of hash if some external dependency changed as well.
   * TODO: need to add requiresAnc dependency here
   */
  private[smv] lazy val hashOfHash : Int = {
    (requiresDS().map(_.hashOfHash) ++ Seq(version, datasetHash)).hashCode()
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
  def dqm(): SmvDQM = SmvDQM()

  /**
   * createDsDqm could be overridden by smv internal SmvDataSet's sub-classes
   */
  private[smv] def createDsDqm() = dqm()

  /**
   * returns the DataFrame from this dataset (file/module).
   * The value is cached so this function can be called repeatedly.
   * Note: the RDD graph is cached and NOT the data (i.e. rdd.cache is NOT called here)
   */
  def rdd() = {
    if (rddCache == null) {
      rddCache = computeRDD
    }
    rddCache
  }

  /** The "versioned" module file base name. */
  private def versionedBasePath(prefix: String): String = {
    val verHex = f"${hashOfHash}%08x"
    s"""${app.smvConfig.outputDir}/${prefix}${name}_${verHex}"""
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
    val csvPath = moduleCsvPath()
    val eddPath = moduleEddPath()
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
  private[smv] def currentModuleOutputFiles() : Seq[String] = {
    Seq(moduleCsvPath(), moduleSchemaPath(), moduleEddPath(), moduleValidPath())
  }

  private[smv] def persist(rdd: DataFrame, prefix: String = "") = {
    val filePath = moduleCsvPath(prefix)
    val fmt = DateTimeFormat.forPattern("HH:mm:ss")

    val counter = app.sc.accumulator(0l)
    val before = DateTime.now()
    println(s"${fmt.print(before)} PERSISTING: ${filePath}")

    val df = rdd.smvPipeCount(counter)
    val handler = new FileIOHandler(app.sqlContext, filePath)

    //Always persist null string as a special value with assumption that it's not
    //a valid data value
    handler.saveAsCsvWithSchema(df, strNullValue = "_SmvStrNull_")

    val after = DateTime.now()
    val runTime = PeriodFormat.getDefault().print(new Period(before, after))
    val n = counter.value

    println(s"${fmt.print(after)} RunTime: ${runTime}, N: ${n}")

    // if EDD flag was specified, generate EDD for the just saved file!
    // Use the "cached" file that was just saved rather than cause an action
    // on the input RDD which may cause some expensive computation to re-occur.
    if (app.genEdd)
      readPersistedFile(prefix).get.edd.persistBesideData(filePath)
  }

  private[smv] def readPersistedFile(prefix: String = ""): Try[DataFrame] = {
    Try({
      val handler = new FileIOHandler(app.sqlContext, moduleCsvPath(prefix))
      handler.csvFileWithSchema(CsvAttributes.defaultCsv)
    })
  }

  private[smv] def computeRDD: DataFrame = {
    val dsDqm = new DQMValidator(createDsDqm())
    val validator = new ValidationSet(Seq(dsDqm), isPersistValidateResult)

    if (isEphemeral) {
      val df = dsDqm.attachTasks(doRun(dsDqm))
      validator.validate(df, false, moduleValidPath()) // no action before this point
      df
    } else {
      readPersistedFile().recoverWith {case e =>
        val df = dsDqm.attachTasks(doRun(dsDqm))
        persist(df)
        validator.validate(df, true, moduleValidPath()) // has already had action (from persist)
        readPersistedFile()
      }.get
    }
  }

  /** path of published data for a given version. */
  private def publishPath(version: String) = s"${app.smvConfig.publishDir}/${version}/${name}.csv"

  /**
   * Publish the current module data to the publish directory.
   * PRECONDITION: user must have specified the --publish command line option (that is where we get the version)
   */
  private[smv] def publish() = {
    val df = rdd()
    val version = app.smvConfig.cmdLine.publish()
    val handler = new FileIOHandler(app.sqlContext, publishPath(version))
    //Same as in persist, publish null string as a special value with assumption that it's not
    //a valid data value
    handler.saveAsCsvWithSchema(df, strNullValue = "_SmvStrNull_")

    /* publish should also calculate edd if generarte Edd flag was turned on */
    if (app.genEdd)
      df.edd.persistBesideData(publishPath(version))
  }

  /**
   * the stage associated with this dataset.  Only valid for modules/files defined in a stage package.
   * Adhoc files/modules will have a null for the stage.
   */
  private[smv] lazy val parentStage : SmvStage = app.stages.findStageForDataSet(this)

  private[smv] def stageVersion() = if (parentStage != null) parentStage.version else None

  /**
   * Read the published data of this module if the parent stage has specified a version.
   * @return Some(DataFrame) if the stage has a version specified, None otherwise.
   */
  private[smv] def readPublishedData() : Option[DataFrame] = {
    stageVersion.map { v =>
      val handler = new FileIOHandler(app.sqlContext, publishPath(v))
      handler.csvFileWithSchema(null)
    }
  }
}

/**
 * Abstract out the common part of input SmvDataSet
 */
private[smv] abstract class SmvInputDataSet extends SmvDataSet {
  override def requiresDS() = Seq.empty
  override val isEphemeral = true

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
case class SmvHiveTable(val tableName: String) extends SmvInputDataSet {
  override def description() = s"Hive Table: @${tableName}"

  override private[smv] def doRun(dsDqm: DQMValidator): DataFrame = {
    val df = app.sqlContext.sql("select * from " + tableName)
    run(df)
  }
}

/**
 * Both SmvFile and SmvCsvStringData shared the parser validation part, extract the
 * common part to the new ABC: SmvDSWithParser
 */
trait SmvDSWithParser extends SmvDataSet {
  val forceParserCheck = true
  val failAtParsingError = true

  override def createDsDqm() =
    if (failAtParsingError) dqm().add(FailParserCountPolicy(1), Option(true))
    else if (forceParserCheck) dqm().addAction()
    else dqm()
}

abstract class SmvFile extends SmvInputDataSet {
  val path: String
  val schemaPath: String = null
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
    if(schemaPath == null) None
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
    val mTime = SmvHDFS.modificationTime(fileName)

    val schemaPath = fullSchemaPath.getOrElse(SmvSchema.dataPathToSchemaPath(fullPath))
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
) extends SmvFile with SmvDSWithParser {

  override private[smv] def doRun(dsDqm: DQMValidator): DataFrame = {
    val parserValidator = dsDqm.createParserValidator()
    // TODO: this should use inputDir instead of dataDir
    val handler = new FileIOHandler(app.sqlContext, fullPath, fullSchemaPath, parserValidator)
    val df = handler.csvFileWithSchema(csvAttributes)
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
) extends SmvFile with SmvDSWithParser {

  override val path = dir

  override def fullSchemaPath = {
    if(schemaPath == null) Option(SmvSchema.dataPathToSchemaPath(fullPath))
    else Option(findFullPath(schemaPath))
  }

  override private[smv] def doRun(dsDqm: DQMValidator): DataFrame = {
    val parserValidator = dsDqm.createParserValidator()

    val filesInDir = SmvHDFS.dirList(fullPath).map{n => s"${fullPath}/${n}"}

    val df = filesInDir.map{s =>
      val handler = new FileIOHandler(app.sqlContext, s, fullSchemaPath, parserValidator)
      handler.csvFileWithSchema(csvAttributes)
    }.reduce(_ unionAll _)

    run(df)
  }
}

case class SmvFrlFile(
    override val path: String,
    override val schemaPath: String = null,
    override val isFullPath: Boolean = false
  ) extends SmvFile with SmvDSWithParser {

  override private[smv] def doRun(dsDqm: DQMValidator): DataFrame = {
    val parserValidator = dsDqm.createParserValidator()
    // TODO: this should use inputDir instead of dataDir
    val handler = new FileIOHandler(app.sqlContext, fullPath, fullSchemaPath, parserValidator)
    val df = handler.frlFileWithSchema()
    run(df)
  }
}

/**
 * Keep this interface for existing application code, will be removed when application code cleaned up
 */
@deprecated("for existing application code", "1.5")
object SmvFile {
  def apply(path: String, csvAttributes: CsvAttributes) =
    new SmvCsvFile(path, csvAttributes)
  def apply(name: String, path: String, csvAttributes: CsvAttributes) =
    new SmvCsvFile(path, csvAttributes)
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
  override val isEphemeral = false

  type runParams = Map[SmvDataSet, DataFrame]
  def run(inputs: runParams) : DataFrame

  /** perform the actual run of this module to get the generated SRDD result. */
  override private[smv] def doRun(dsDqm: DQMValidator): DataFrame = {
    // TODO turn on dependency check by uncomment the following line after test against projects
    // checkDependency()
    run(requiresDS().map(r => (r, app.resolveRDD(r))).toMap)
  }

  /** Use Bytecode analysis to figure out dependency and check against
   *  requiresDS and requiresAnc. Could consider to totaly drop requiresDS and
   *  requiresAnc, and always use ASM to derive the dependency
   **/
  private def checkDependency(): Unit = {
    val dep = DataSetDependency(this.getClass.getName)
    dep.dependsAnc.
      map{s => (s, SmvReflection.objectNameToInstance[SmvAncillary](s))}.
      filterNot{case (s,a) => requiresAnc().contains(a)}.
      foreach{case (s, a) =>
        throw new IllegalArgumentException(s"SmvAncillary ${s} need to be specified in requiresAnc")
      }
    dep.dependsDS.
      map{s => (s, SmvReflection.objectNameToInstance[SmvDataSet](s))}.
      filterNot{case (s,a) => requiresDS().contains(a)}.
      foreach{case (s, a) =>
        throw new IllegalArgumentException(s"SmvDataSet ${s} need to be specified in requiresDS, ${a}")
      }
  }

  /**
   * Create a snapshot in the current module at some result DataFrame.
   * This is useful for debugging a long SmvModule by creating snapshots along the way.
   * {{{
   * object MyMod extends SmvModule("...") {
   *   override def requireDS = Seq(...)
   *   override def run(...) = {
   *      val s1 = ...
   *      snapshot(s1, "s1")
   *      val s2 = f(s1)
   *      snapshot(s2, "s2")
   *      ...
   *   }
   * }}}
   */
  def snapshot(df: DataFrame, prefix: String) : DataFrame = {
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
class SmvModuleLink(outputModule: SmvOutput) extends
  SmvModule(s"Link to ${outputModule.asInstanceOf[SmvDataSet].name}") {

  private[smv] val smvModule = outputModule.asInstanceOf[SmvDataSet]

  /**
   *  No need to check isEphemeral any more
   *  SmvOutput will be published anyhow regardless of ephemeral or not
   **/
  // require(! smvModule.isEphemeral)
  // TODO: add check that the link is to an object in a different stage!!!

  private[smv] val isFollowLink = true

  override val isEphemeral = true

  /**
   * override the module run/requireDS methods to be a no-op as it will never be called (we overwrite doRun as well.)
   */
  override def requiresDS() = Seq.empty
  override def run(inputs: runParams) = null

  /**
   * If the depended smvModule has a published version, SmvModuleLink's datasetHash
   * depends on the version string. Otherwise, depends on the smvModule's hashOfHash
   **/
  override def datasetHash() = {
    val dependedHash = smvModule.stageVersion.map{v =>
      val crc = new java.util.zip.CRC32
      crc.update(v.toCharArray.map(_.toByte))
      (crc.getValue).toInt
    }.getOrElse(smvModule.hashOfHash)

    (dependedHash + datasetCRC).toInt
  }

  /**
   * "Running" a link requires that we read the published output from the upstream `DataSet`.
   * When publish version is specified, it will try to read from the published dir. Otherwise
   * it will either "follow-the-link", which means resolve the modules the linked DS depends on
   * and run the DS, or "not-follow-the-link", which will try to read from the persisted data dir
   * and fail if not found.
   */
  override private[smv] def doRun(dsDqm: DQMValidator): DataFrame = {
    if (isFollowLink) {
      smvModule.readPublishedData().getOrElse(smvModule.rdd())
    } else {
      smvModule.readPublishedData().
        orElse { smvModule.readPersistedFile().toOption }.
        getOrElse(throw new IllegalStateException(s"can not find published or persisted ${description}"))
    }
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
  ) extends SmvDataSet with SmvDSWithParser {

  override def description() = s"Dummy module to create DF from strings"
  override def requiresDS() = Seq.empty
  override val isEphemeral = true

  override def datasetHash() = {
    val crc = new java.util.zip.CRC32
    crc.update((schemaStr + data).toCharArray.map(_.toByte))
    (crc.getValue + datasetCRC).toInt
  }

  override def doRun(dsDqm: DQMValidator): DataFrame = {
    val schema = SmvSchema.fromString(schemaStr)
    val dataArray = data.split(";").map(_.trim)

    val parserValidator = dsDqm.createParserValidator()
    val handler = new FileIOHandler(app.sqlContext, null, None, parserValidator)
    handler.csvStringRDDToDF(app.sc.makeRDD(dataArray), schema, schema.extractCsvAttributes())
  }
}

/**
 * A marker trait that indicates that a SmvDataSet/SmvModule decorated with this trait is an output DataSet/module.
 */
trait SmvOutput { this : SmvDataSet =>
}

/** Base marker trait for run configuration objects */
trait SmvRunConfig

/**
 * SmvDataSet that can be configured to return different DataFrames.
 */
trait Using[+T <: SmvRunConfig] {
  self: SmvDataSet =>

  /** The actual run configuration object */
  lazy val runConfig: T = {
    val confObj = self.app.smvConfig.runConfObj

    require(confObj.isDefined,
      s"Expected a run configuration object provided with ${SmvConfig.RunConfObjKey} but found none")


    import scala.reflect.runtime.{universe => ru}
    val mir = ru.runtimeMirror(getClass.getClassLoader)

    val sym = mir.staticModule(confObj.get)
    val module = mir.reflectModule(sym)
    module.instance.asInstanceOf[T]
  }
}
