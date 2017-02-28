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

import org.tresamigos.smv.class_loader.SmvClassLoader
import org.tresamigos.smv.shell.EddCompare
import dqm.DQMValidator

import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.{SparkContext, SparkConf}

import scala.collection.mutable
import scala.collection.JavaConversions._
import scala.util.control.NonFatal
import scala.util.{Try, Success, Failure}

/**
 * Driver for SMV applications.  Most apps do not need to override this class and should just be
 * launched using the SmvApp object (defined below)
 */
class SmvApp (private val cmdLineArgs: Seq[String], _sc: Option[SparkContext] = None, _sql: Option[SQLContext] = None) extends SmvPackageManager {

  val smvConfig = new SmvConfig(cmdLineArgs)
  val genEdd = smvConfig.cmdLine.genEdd()
  val publishHive = smvConfig.cmdLine.publishHive()
  val stages = smvConfig.stages
  val sparkConf = new SparkConf().setAppName(smvConfig.appName)

  /** Register Kryo Classes
   * Since none of the SMV classes will be put in an RDD, register them or not does not make
   * significant performance improvement
   *
   * val allSerializables = SmvReflection.objectsInPackage[Serializable]("org.tresamigos.smv")
   * sparkConf.registerKryoClasses(allSerializables.map{_.getClass}.toArray)
   **/

  val sc = _sc.getOrElse(new SparkContext(sparkConf))
  val sqlContext = _sql.getOrElse(new org.apache.spark.sql.hive.HiveContext(sc))

  // dsm should be private but will be temporarily public to accomodate outside invocations
  val dsm = new DataSetMgr(smvConfig, SmvApp.DependencyRules)
  def registerRepoFactory(factory: DataSetRepoFactory): Unit =
    dsm.register(factory)
  registerRepoFactory(new DataSetRepoFactoryScala(smvConfig))

  // Since OldVersionHelper will be used by executors, need to inject the version from the driver
  OldVersionHelper.version = sc.version

  // configure spark sql params and inject app here rather in run method so that it would be done even if we use the shell.
  setSparkSqlConfigParams()

  /** stack of items currently being resolved.  Used for cyclic checks. */
  val resolveStack: mutable.Stack[String] = mutable.Stack()

  override def getAllPackageNames = stages.stages flatMap (_.getAllPackageNames)
  override lazy val allDatasets = dsm.load(dsm.allDataSets:_*)

  override lazy val allOutputModules =
    dsm.load(dsm.allOutputModules:_*) map (_.asInstanceOf[SmvModule])

  override lazy val predecessors =
    allDatasets.map {
      case d: SmvModuleLink => (d, Seq(d.smvModule))
      case d: SmvDataSet => (d, d.resolvedRequiresDS)
    }.toMap

  // Issue # 349: look up stage by the dataset's name instead of the
  // object identity because after hot-deploy in shell via a new
  // classloader, the same datset no longer has the same object
  // instance.
  lazy val dsname2stage: Map[String, SmvStage] =
    (for {
      st <- stages.stages
      ds <- st.allDatasets
    } yield (ds.fqn, st)).toMap

  /**
   * Find the stage that a given dataset belongs to.
   */
  def findStageForDataSet(ds: SmvDataSet) : Option[SmvStage] = dsname2stage.get(ds.fqn)

  /**
   * Create a DataFrame from string for temporary use (in test or shell)
   * By default, don't persist validation result
   **/
  def createDF(schemaStr: String, data: String, isPersistValidateResult: Boolean = false) = {
    val smvCF = SmvCsvStringData(schemaStr, data, isPersistValidateResult)
    smvCF.rdd
  }

  /** all modules known to this app. */
  private[smv] def allAppModules = stages.allModules

  /** list of all current valid output files in the output directory. All other files in output dir can be purged. */
  private[smv] def validFilesInOutputDir() : Seq[String] = {
    allAppModules.
      flatMap(_.currentModuleOutputFiles).
      map(SmvHDFS.baseName(_))
  }

  /** remove all non-current files in the output directory */
  private[smv] def purgeOldOutputFiles() = {
    if (smvConfig.cmdLine.purgeOldOutput())
      SmvHDFS.purgeDirectory(smvConfig.outputDir, validFilesInOutputDir())
  }

  /**
   * Does the dataset follow dependency rules?
   *
   * TODO: need to figure out if we should enforce dependency rules
   * across modules written in different languages, and how if we
   * should.  Right now we skip external dataset references.
   */
  def checkDependencyRules(ds: SmvDataSet): Seq[DependencyViolation] = {
    val results = SmvApp.DependencyRules map (_.check(ds))
    results.foldRight(Seq.empty[DependencyViolation])((elem, acc) => elem.toSeq ++: acc)
  }

  /** Textual representation for output to console */
  def mkViolationString(violations: Seq[DependencyViolation], indent: String = ".."): String =
    (for {
      v <- violations
      header = s"${indent} ${v.description}"
    } yield
      (header +: v.components.map(m => s"${indent}${indent} ${m.urn}")).mkString("\n")
    ).mkString("\n")

  /**
   * Get the DataFrame associated with data set. The DataFrame plan (not data) is cached in
   * dfCache the to ensure only a single DataFrame exists for a given data set
   * (file/module).
   */
  val dfCache: mutable.Map[String, DataFrame] = mutable.Map.empty[String, DataFrame]
  def resolveRDD(ds: SmvDataSet): DataFrame = {
    val vFqn = ds.versionedFqn
    Try( dfCache(vFqn) ) match {
      case Success(df) => df
      case Failure(_) =>
        val df = ds.rdd
        dfCache += (vFqn -> df)
        df
    }
  }

  lazy val packagesPrefix = {
    val m = allAppModules
    if (m.isEmpty) ""
    else m.map(_.fqn).reduce{(l,r) =>
        (l.split('.') zip r.split('.')).
          collect{ case (a, b) if (a==b) => a}.mkString(".")
      } + "."
  }

  /** clean name in graph output */
  private[smv] def moduleNameForPrint(ds: SmvDataSet) = ds.fqn.stripPrefix(packagesPrefix)

  private def genDotGraph(module: SmvModule) = {
    val pathName = s"${module.fqn}.dot"
    val graphString = new graph.SmvGraphUtil(this, stages).createGraphvisCode(Seq(module))
    SmvReportIO.saveLocalReport(graphString, pathName)
  }

  def genJSON(packages: Seq[String] = Seq()) = {
    val pathName = s"${smvConfig.appName}.json"

    val stagesToGraph = packages.map{stages.findStage(_)}
    val graphString = new graph.SmvGraphUtil(this, new SmvStages(stagesToGraph)).createGraphJSON()

    SmvReportIO.saveLocalReport(graphString, pathName)
  }

  /**
   * pass on the spark sql props set in the smv config file(s) to spark.
   * This is just for convenience so user can manage both smv/spark props in a single file.
   */
  private def setSparkSqlConfigParams() = {
    for ((key, value) <- smvConfig.sparkSqlProps) {
      sqlContext.setConf(key, value)
    }
  }

  /**
   * delete the current output files of the modules to run (and not all the intermediate modules).
   */
  private def deleteOutputModules() = {
    // TODO: replace with df.write.mode(Overwrite) once we move to spark 1.4
    modulesToRun foreach {m => m.deleteOutputs()}
  }

  /**
   * generate dependency graphs if "-g" flag was specified on command line.
   * @return true of graphs were generated otherwise return false.
   */
  private def generateDependencyGraphs() : Boolean = {
    if (smvConfig.cmdLine.graph()) {
      modulesToRun foreach { module =>
        // TODO: need to combine the modules for graphs into a single graph.
        genDotGraph(module)
      }
      true
    } else {
      false
    }
  }

  /**
   * compare EDD results if the --edd-compare flag was specified with edd files to compare.
   * @return true if edd files were compared, otherwise false.
   */
  private def compareEddResults() : Boolean = {
    smvConfig.cmdLine.compareEdd.map { eddsToCompare =>
      val edd1 = eddsToCompare(0)
      val edd2 = eddsToCompare(1)
      val (passed, log) = EddCompare.compareFiles(edd1, edd2)
      if (passed) {
        println("EDD Results are the same")
      } else {
        println("EDD Results differ:")
        println(log)
      }
      true
    }.orElse(Some(false))()
  }

  private def generateGraphJSON(): Boolean = {
    smvConfig.cmdLine.json.map { stageList =>
      genJSON(stageList)
      true
    }.orElse(Some(false))()
  }

  def generateAllGraphJSON() = {
    genJSON(stages.stageNames)
  }

  /**
   * if the publish to hive flag is set, the publish
   */
  def publishModulesToHive() : Boolean = {
    false
    // if (publishHive) {
    //   // TODO: add call to publish a specific module.
    //   //smvConfig.modulesToRun().foreach { module => module.publish() }
    //   true
    // } else {
    //   false
    // }
  }

  /**
   * Publish the specified modules if the "--publish" flag was specified on command line.
   * @return true if modules were published, otherwise return false.
   */
  private def publishOutputModules() : Boolean = {
    if (smvConfig.cmdLine.publish.isDefined) {
      modulesToRun foreach { module => module.publish() }
      true
    } else {
      false
    }
  }

  /**
   * run the specified output modules.
   * @return true if modules were generated, otherwise false.
   */
  private def generateOutputModules() : Boolean = {
    deleteOutputModules()

    modulesToRun foreach { module =>
      val modResult = runModule(module.urn)

      // if module was ephemeral, then it was not saved during graph execution and we need
      // to persist it here explicitly.
      if (module.isEphemeral)
        module.persist(modResult)
    }

    modulesToRun.nonEmpty
  }

  /** The "versioned" module file base name. */
  private def versionedPath(suffix: String, prefix: String = "")(name: String, hash: Int): String = {
    val verHex = f"${hash}%08x"
    s"""${smvConfig.outputDir}/${prefix}${name}_${verHex}.${suffix}"""
  }

  /** Returns the path for the module's csv output */
  val moduleCsvPath = versionedPath("csv") _

  /** Returns the path for the module's schema file */
  private[smv] val moduleSchemaPath = versionedPath("schema") _

  /** Returns the path for the module's edd report output */
  private[smv] val moduleEddPath = versionedPath("edd") _

  /** Returns the path for the module's reject report output */
  private[smv] val moduleValidPath = versionedPath("valid") _

  /** Path for published module of a specified version */
  private[smv] def publishPath(name: String, version: String) =
    s"""${smvConfig.publishDir}/${version}/${name}.csv"""

  /** Run a module by its fully qualified name in its respective language environment */
  def runModule(modUrn: String): DataFrame = resolveRDD(dsm.load(URN(modUrn)).head)

  @inline private def notfound(modUrn: String) = throw new SmvRuntimeException(s"Cannot find module ${modUrn}")

  /**
   * Publish the result of an SmvModule
   */
  def publishModule(modFqn: String, version: String): Unit = {
    println(s"publishing module ${modFqn}")
    val df = runModule("mod:"+modFqn)
    val path = publishPath(modFqn, version)
    println(s"publish path is ${path}")
    SmvUtil.publish(sqlContext, df, path, genEdd)
  }

  /**
   * Run a module given it's name.  This is mostly used by SparkR to resolve modules.
   */
  def runModuleByName(modName: String) : DataFrame = {
    val mod = dsm.load(dsm.inferURN(modName)).asInstanceOf[SmvModule]
    resolveRDD(mod)
  }

  /**
   * sequence of SmvModules to run based on the command line arguments.
   * Returns the union of -a/-m/-s command line flags.
   */
  private[smv] def modulesToRun() : Seq[SmvModule] = {
    val cmdline = smvConfig.cmdLine
    val empty = Some(Seq.empty[String])

    val directModURNs: Seq[URN] =
      cmdline.modsToRun.orElse(empty)().map (dsm.inferURN(_))

    val stageModURNs =
      cmdline.stagesToRun.orElse(empty)() flatMap {dsm.outputModsForStage}

    val appModURNs = if (cmdline.runAllApp()) dsm.allOutputModules else Seq.empty[URN]

    val allURNs = (directModURNs ++ stageModURNs ++ appModURNs).distinct

    dsm.load(allURNs:_*) map (_.asInstanceOf[SmvModule])
  }

  /**
   * The main entry point into the app.  This will parse the command line arguments
   * to determine which modules should be run/graphed/etc.
   */
  def run() = {
    val mods = modulesToRun
    if (mods.nonEmpty) {
      println("Modules to run/publish")
      println("----------------------")
      println(mods.map(_.fqn).mkString("\n"))
      println("----------------------")
    }

    purgeOldOutputFiles()

    // either generate graphs, publish modules, or run output modules (only one will occur)
    compareEddResults() || generateGraphJSON() || generateDependencyGraphs() || publishModulesToHive() ||  publishOutputModules() || generateOutputModules()
  }
}

/**
 * Common entry point for all SMV applications.  This is the object that should be provided to spark-submit.
 */
object SmvApp {
  var app: SmvApp = _

  val DependencyRules: Seq[DependencyRule] = Seq(SameStageDependency, LinkFromDiffStage)

  def init(args: Array[String], _sc: Option[SparkContext] = None, _sql: Option[SQLContext] = None) = {
    app = new SmvApp(args, _sc, _sql)
    app
  }

  /**
   * Creates a new app instances from a sql context.  This is used by SparkR to create a new app.
   */
  def newApp(sqlContext: SQLContext, appPath: String) : SmvApp = {
    SmvApp.init(
      Seq("-m", "None", "--smv-app-dir", appPath).toArray,
      Option(sqlContext.sparkContext),
      Option(sqlContext))
    SmvApp.app
  }

  def main(args: Array[String]) {
    init(args)
    app.run()
  }
}
