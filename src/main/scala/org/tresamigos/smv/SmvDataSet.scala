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

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.sql.catalyst.expressions.{GenericMutableRow, Row}

import scala.util.Try
import org.joda.time._
import org.joda.time.format._

/**
 * Dependency management unit within the SMV application framework.  Execution order within
 * the SMV application framework is derived from dependency between SmvDataSet instances.
 * Instances of this class can either be a file or a module. In either case, there would
 * be a single result DataFrame.
 */
abstract class SmvDataSet {

  var app: SmvApp = _
  private var rddCache: DataFrame = null

  def name(): String
  def description(): String

  /** modules must override to provide set of datasets they depend on. */
  def requiresDS() : Seq[SmvDataSet]

  /** user tagged code "version".  Derived classes should update the value when code or data */
  def version() : Int = 0

  /** CRC computed from the dataset "code" (not data) */
  def classCodeCRC() : Int = 0

  def validations(): ValidationSet = new ValidationSet(Nil)
  /**
   * Determine the hash of this module and the hash of hash (HOH) of all the modules it depends on.
   * This way, if this module or any of the modules it depends on changes, the HOH should change.
   * The "version" of the current dataset is also used in the computation to allow user to force
   * a change in the hash of hash if some external dependency changed as well.
   */
  private[smv] lazy val hashOfHash : Int = {
    (requiresDS().map(_.hashOfHash) ++ Seq(version, classCodeCRC)).hashCode()
  }

  /**
   * flag if this module is ephemeral or short lived so that it will not be persisted when a graph is executed.
   * This is quite handy for "filter" or "map" type modules so that we don't force an extra I/O step when it
   * is not needed.  By default all modules are persisted unless the flag is overriden to true.
   * Note: the module will still be persisted if it was specifically selected to run by the user.
   */
  def isEphemeral: Boolean

  /**
   * returns the DataFrame from this dataset (file/module).
   * The value is cached so this function can be called repeatedly.
   * Note: the RDD graph is cached and NOT the data (i.e. rdd.cache is NOT called here)
   */
  def rdd() = {
    if (rddCache == null)
      rddCache = computeRDD
    rddCache
  }

  /**
   * Inject the given app into this dataset and all datasets that this depends on.
   * Note: only the first setting is honored (as an optimization)
   */
  def injectApp(_app: SmvApp) : Unit = {
    // short circuit the setting if this was already done.
    if (app == null) {
      app = _app
      requiresDS().foreach( ds => ds.injectApp(app))
    }
  }

  /** The "versioned" module file base name. */
  private def versionedBasePath(prefix: String): String = {
    val verHex = f"${hashOfHash}%08x"
    s"""${app.smvConfig.outputDir}/${prefix}${name}_${verHex}"""
  }

  /** Returns the path for the module's csv output */
  private[smv] def moduleCsvPath(prefix: String = ""): String =
    versionedBasePath(prefix) + ".csv"

  /** Returns the path for the module's schema file */
  private[smv] def moduleSchemaPath(prefix: String = ""): String =
    versionedBasePath(prefix) + ".schema"

  /** Returns the path for the module's edd report output */
  private[smv] def moduleEddPath(prefix: String = ""): String =
    versionedBasePath(prefix) + ".edd"

  /** Returns the path for the module's reject report output */
  private[smv] def moduleRejectPath(prefix: String = ""): String =
    versionedBasePath(prefix) + ".reject"

  /** perform the actual run of this module to get the generated SRDD result. */
  private[smv] def doRun(): DataFrame

  /**
   * delete the output(s) associated with this module (csv file and schema).
   * TODO: replace with df.write.mode(Overwrite) once we move to spark 1.4
   */
  private[smv] def deleteOutputs() = {
    val csvPath = moduleCsvPath()
    val eddPath = moduleEddPath()
    val schemaPath = moduleSchemaPath()
    val rejectPath = moduleRejectPath()
    SmvHDFS.deleteFile(csvPath)
    SmvHDFS.deleteFile(schemaPath)
    SmvHDFS.deleteFile(eddPath)
    SmvHDFS.deleteFile(rejectPath)
  }

  /**
   * Returns current valid outputs produced by this module.
   */
  private[smv] def currentModuleOutputFiles() : Seq[String] = {
    Seq(moduleCsvPath(), moduleSchemaPath(), moduleEddPath(), moduleRejectPath())
  }

  private[smv] def persist(rdd: DataFrame, prefix: String = "") = {
    val filePath = moduleCsvPath(prefix)
    implicit val ca = CsvAttributes.defaultCsvWithHeader
    val fmt = DateTimeFormat.forPattern("HH:mm:ss")

    val counter = new ScCounter(app.sc)
    val before = DateTime.now()
    println(s"${fmt.print(before)} PERSISTING: ${filePath}")

    rdd.smvPipeCount(counter).saveAsCsvWithSchema(filePath)

    val after = DateTime.now()
    val runTime = PeriodFormat.getDefault().print(new Period(before, after))
    val n = counter("N")

    println(s"${fmt.print(after)} RunTime: ${runTime}, N: ${n}")

    // if EDD flag was specified, generate EDD for the just saved file!
    // Use the "cached" file that was just saved rather than cause an action
    // on the input RDD which may cause some expensive computation to re-occur.
    if (app.genEdd)
      readPersistedFile(prefix).get.edd.addBaseTasks().saveReport(moduleEddPath(prefix))
  }

  private[smv] def readPersistedFile(prefix: String = ""): Try[DataFrame] = {
    implicit val ca = CsvAttributes.defaultCsv
    Try({
      val smvFile = SmvCsvFile(moduleCsvPath(prefix), ca)
      smvFile.injectApp(this.app)
      smvFile.rdd
    })
  }

  def computeRDD: DataFrame = {
    val (df, hasActionYet) = if(isEphemeral) {
      (doRun(), false)
    } else {
      val resultDf = readPersistedFile().recoverWith {case e =>
        persist(doRun())
        readPersistedFile()
      }.get
      (resultDf, true)
    }

    validations.validate(df, hasActionYet)
    df
  }
}

abstract class SmvFile extends SmvDataSet {
  val basePath: String
  override def description() = s"Input file: @${basePath}"
  override def requiresDS() = Seq.empty
  override val isEphemeral = true
  val failAtParsingError = true

  //TODO: split this to 2
  lazy val parserValidator = new ParserValidation(app.sc, failAtParsingError)

  override def validations() = new ValidationSet(Seq(parserValidator))

  def fullPath = {
    if (("""^[\.\/]"""r).findFirstIn(basePath) != None) basePath
    else if (app == null) throw new IllegalArgumentException(s"app == null and $basePath is not an absolute path")
    else s"${app.smvConfig.dataDir}/${basePath}"
  }

  override def classCodeCRC() = {
    val fileName = fullPath
    val mTime = SmvHDFS.modificationTime(fileName)
    val crc = new java.util.zip.CRC32
    crc.update(fileName.toCharArray.map(_.toByte))
    (crc.getValue + mTime).toInt
  }

  override def name() = {
    val nameRex = """(.+)(.csv)*(.gz)*""".r
    basePath match {
      case nameRex(v, _, _) => v
      case _ => throw new IllegalArgumentException(s"Illegal base path format: $basePath")
    }
  }

  protected def seqStringRDDToDF(
    sqlContext: SQLContext,
    rdd: RDD[Seq[String]],
    schema: SmvSchema,
    parserV: ParserValidation
    //rejects: RejectLogger
  ) = {
    val add: (Exception, String) => Unit = {(e,r) => parserV.addWithReason(e,r)}
    val rowRdd = rdd.mapPartitions{ iterator =>
      val mutableRow = new GenericMutableRow(schema.getSize)
      iterator.map { r =>
        try {
          require(r.size == schema.getSize)
          for (i <- 0 until schema.getSize) {
            mutableRow.update(i, schema.toValue(i, r(i)))
          }
          Some(mutableRow)
        } catch {
          case e:IllegalArgumentException  => add(e, r.mkString(",")); None
          case e:NumberFormatException  => add(e, r.mkString(",")); None
          case e:java.text.ParseException  =>  add(e, r.mkString(",")); None
        }
      }.collect{case Some(l) => l.asInstanceOf[Row]}
    }
    sqlContext.createDataFrame(rowRdd, schema.toStructType)
  }

  def run(df: DataFrame) = df
}

/**
 * Represents a raw input file with a given file path (can be local or hdfs) and CSV attributes.
 */
case class SmvCsvFile(
  basePath: String,
  csvAttributes: CsvAttributes,
  schemaPath: Option[String] = None
) extends SmvFile {

  private[smv] def csvStringRDDToDF(
    sqlContext: SQLContext,
    rdd: RDD[String],
    schema: SmvSchema,
    parserV: ParserValidation
  ) = {
    val parser = new CSVStringParser[Seq[String]]((r:String, parsed:Seq[String]) => parsed, parserV)
    val _ca = csvAttributes
    val seqStringRdd = rdd.mapPartitions{ parser.parseCSV(_)(_ca) }
    seqStringRDDToDF(sqlContext, seqStringRdd, schema, parserV)
  }

  private def csvFileWithSchema(
    dataPath: String,
    schemaPath: String
  ): DataFrame = {
    val sc = app.sqlContext.sparkContext
    val schema = SmvSchema.fromFile(sc, schemaPath)

    val strRDD = sc.textFile(dataPath)
    val noHeadRDD = if (csvAttributes.hasHeader) CsvAttributes.dropHeader(strRDD) else strRDD

    csvStringRDDToDF(app.sqlContext, noHeadRDD, schema, parserValidator)
  }

  private[smv] def doRun(): DataFrame = {
    // TODO: this should use inputDir instead of dataDir
    val sp = schemaPath.getOrElse(SmvSchema.dataPathToSchemaPath(fullPath))
    val df = csvFileWithSchema(fullPath, sp)
    run(df)
  }
}

case class SmvFrlFile(
    basePath: String,
    schemaPath: Option[String] = None
  ) extends SmvFile {

  def frlStringRDDToDF(
    sqlContext: SQLContext,
    rdd: RDD[String],
    schema: SmvSchema,
    slices: Seq[Int],
    parserV: ParserValidation
  ) = {
    val startLen = slices.scanLeft(0){_ + _}.dropRight(1).zip(slices)
    val parser = new CSVStringParser[Seq[String]]((r:String, parsed:Seq[String]) => parsed, parserV)
    val seqStringRdd = rdd.mapPartitions{ parser.parseFrl(_, startLen) }
    seqStringRDDToDF(sqlContext, seqStringRdd, schema, parserV)
  }

  private def frlFileWithSchema(dataPath: String, schemaPath: String): DataFrame = {
    val sc = app.sqlContext.sparkContext
    val slices = SmvSchema.slicesFromFile(sc, schemaPath)
    val schema = SmvSchema.fromFile(sc, schemaPath)
    require(slices.size == schema.getSize)

    // TODO: show we allow header in Frl files?
    val strRDD = sc.textFile(dataPath)
    frlStringRDDToDF(app.sqlContext, strRDD, schema, slices, parserValidator)
  }

  private[smv] def doRun(): DataFrame = {
    // TODO: this should use inputDir instead of dataDir
    val sp = schemaPath.getOrElse(SmvSchema.dataPathToSchemaPath(fullPath))
    val df = frlFileWithSchema(fullPath, sp)
    run(df)
  }
}

/** Keep this interface for existing application code, will be removed when application code cleaned up */
object SmvFile {
  def apply(basePath: String, csvAttributes: CsvAttributes) =
    new SmvCsvFile(basePath, csvAttributes)
  def apply(name: String, basePath: String, csvAttributes: CsvAttributes) =
    new SmvCsvFile(basePath, csvAttributes)
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

  override val name = this.getClass().getName().filterNot(_=='$')

  /**
   * flag if this module is ephemeral or short lived so that it will not be persisted when a graph is executed.
   * This is quite handy for "filter" or "map" type modules so that we don't force an extra I/O step when it
   * is not needed.  By default all modules are persisted unless the flag is overriden to true.
   * Note: the module will still be persisted if it was specifically selected to run by the user.
   */
  override val isEphemeral = false

  /**
   * module code CRC.  No need to cache the value here as ClassCRC caches it for us (lazy eval)
   */
  val moduleCRC = ClassCRC(this.getClass.getName)

  override def classCodeCRC() : Int = moduleCRC.crc.toInt

  type runParams = Map[SmvDataSet, DataFrame]
  def run(inputs: runParams) : DataFrame

  /** perform the actual run of this module to get the generated SRDD result. */
  private[smv] def doRun(): DataFrame = {
    run(requiresDS().map(r => (r, app.resolveRDD(r))).toMap)
  }

  /**
   * Create a snapshot in the current module at some result DataFrame.
   * This is useful for debugging a long SmvModule by creating snapshots along the way.
   */
  def snapshot(df: DataFrame, prefix: String) : DataFrame = {
    persist(df, prefix)
    readPersistedFile(prefix).get
  }
}

/**
 * A marker trait that indicates that a module decorated with this trait is an output module.
 */
trait SmvOutput {
  this : SmvModule =>
}
