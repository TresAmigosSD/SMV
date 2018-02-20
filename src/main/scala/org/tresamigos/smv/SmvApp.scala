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

import org.tresamigos.smv.util.Edd

import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.{SparkContext, SparkConf}

import scala.collection.mutable
import scala.util.{Try, Success, Failure}
import java.util.List
import collection.JavaConverters._

import org.joda.time.DateTime


/**
 * Driver for SMV applications.  Most apps do not need to override this class and should just be
 * launched using the SmvApp object (defined below)
 */
class SmvApp(private val cmdLineArgs: Seq[String],
             _sc: Option[SparkContext] = None,
             _sql: Option[SQLContext] = None) {

  val smvConfig   = new SmvConfig(cmdLineArgs)
  val genEdd      = smvConfig.cmdLine.genEdd()
  val publishHive = smvConfig.cmdLine.publishHive()
  val publishJDBC = smvConfig.cmdLine.publishJDBC()

  def stages      = smvConfig.stageNames
  val sparkConf   = new SparkConf().setAppName(smvConfig.appName)

  /** Register Kryo Classes
   * Since none of the SMV classes will be put in an RDD, register them or not does not make
   * significant performance improvement
   *
   * val allSerializables = SmvReflection.objectsInPackage[Serializable]("org.tresamigos.smv")
   * sparkConf.registerKryoClasses(allSerializables.map{_.getClass}.toArray)
   **/
  val sc         = _sc.getOrElse(new SparkContext(sparkConf))
  val sqlContext = _sql.getOrElse(new org.apache.spark.sql.hive.HiveContext(sc))

  // dsm should be private but will be temporarily public to accomodate outside invocations
  val dsm = new DataSetMgr(smvConfig)
  def registerRepoFactory(factory: DataSetRepoFactory): Unit =
    dsm.register(factory)
  registerRepoFactory(new DataSetRepoFactoryScala(smvConfig))

  // Since OldVersionHelper will be used by executors, need to inject the version from the driver
  OldVersionHelper.version = sc.version

  // configure spark sql params and inject app here rather in run method so that it would be done even if we use the shell.
  setSparkSqlConfigParams()

  /**
   * Create a DataFrame from string for temporary use (in test or shell)
   * By default, don't persist validation result
   *
   * Passing null for data will create an empty dataframe with a specified schema.
   **/
  def createDF(schemaStr: String, data: String = null, isPersistValidateResult: Boolean = false) = {
    val smvCF = SmvCsvStringData(schemaStr, data, isPersistValidateResult)
    smvCF.rdd(collector=new SmvRunInfoCollector)
  }

  lazy val allDataSets = dsm.allDataSets

  /** list of all current valid output files in the output directory. All other files in output dir can be purged. */
  private[smv] def validFilesInOutputDir(): Seq[String] =
    allDataSets.flatMap(_.allOutputFiles).map(SmvHDFS.baseName(_))

  /**
   * list of all the files with specific suffix in the given directory
   **/
  def getFileNamesByType(dirName: String, suffix: String): List[String] =
    SmvHDFS.dirList(dirName).filter(f => f.endsWith(suffix)).asJava

  /** remove all non-current files in the output directory */
  private[smv] def purgeOldOutputFiles() = {
    if (smvConfig.cmdLine.purgeOldOutput()) {
      SmvHDFS.purgeDirectory(smvConfig.outputDir, validFilesInOutputDir()) foreach {
        case (fn, success) =>
          println(
            if (success) s"... Deleted ${fn}"
            else s"... Unabled to delete ${fn}"
          )
      }
    }
  }

  /**
   * Remove all current files (if any) in the output directory if --force-run-all
   * argument was specified at the commandline
   */
  private[smv] def purgeCurrentOutputFiles() = {
    if (smvConfig.cmdLine.forceRunAll())
      deletePersistedResults(modulesToRunWithAncestors)
  }

  /**
   * Get the DataFrame associated with data set. The DataFrame plan (not data) is cached in
   * dfCache the to ensure only a single DataFrame exists for a given data set
   * (file/module).
   * Note: this keyed by the "versioned" dataset FQN.
   */
  var dfCache: mutable.Map[String, DataFrame] = mutable.Map.empty[String, DataFrame]

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
   * For each module, delete its persisted csv and schema (if any) with the
   * modules current hash
   */

  private[smv] def deletePersistedResults(dsList: Seq[SmvDataSet]) =
    dsList foreach (ds => ds.deleteOutputs(ds.versionedOutputFiles))

  def printDeadModules = {
    if(smvConfig.cmdLine.printDeadModules()) {
      val gu = new graph.SmvGraphUtil(this)
      println("Dead modules by stage:")
      println(gu.createDeadDSList())
      println()
      true
    } else {
      false
    }
  }

  /** Returns the app-level dependency graph as a dot string */
  def dependencyGraphDotString(stageNames: Seq[String] = stages): String =
    new graph.SmvGraphUtil(this, stageNames).createGraphvisCode(modulesToRun)

  /**
   * generate dependency graphs if "-g" flag was specified on command line.
   * @return true if graph were generated otherwise return false.
   */
  private def generateDotDependencyGraph() : Boolean = {
    if (smvConfig.cmdLine.graph()) {
      val pathName = s"${smvConfig.appName}.dot"
      SmvReportIO.saveLocalReport(dependencyGraphDotString(stages), pathName)
      true
    } else {
      false
    }
  }

  /** Returns the app-level dependency graph as a json string */
  def dependencyGraphJsonString(stageNames: Seq[String] = stages): String = {
    new graph.SmvGraphUtil(this, stageNames).createGraphJSON()
  }

  /**
   * generate JSON dependency graphs if "--json" flag was specified on command line.
   * @return true if json graph were generated otherwise return false.
   */
  private def generateJsonDependencyGraph() : Boolean = {
    if (smvConfig.cmdLine.jsonGraph()) {
      val pathName = s"${smvConfig.appName}.json"
      SmvReportIO.saveLocalReport(dependencyGraphJsonString(), pathName)
      true
    } else {
      false
    }
  }

  /**
   * zero parameter wrapper around dependencyGraphJsonString that can be called from python directly.
   * TODO: remove this once we pass args to dependencyGraphJsonString
   */
  def generateAllGraphJSON() = {
    dependencyGraphJsonString()
  }

  /**
   * compare EDD results if the --edd-compare flag was specified with edd files to compare.
   * @return true if edd files were compared, otherwise false.
   */
  private def compareEddResults(): Boolean = {
    smvConfig.cmdLine.compareEdd
      .map { eddsToCompare =>
        val edd1          = eddsToCompare(0)
        val edd2          = eddsToCompare(1)
        val (passed, log) = util.Edd.compareFiles(edd1, edd2)
        if (passed) {
          println("EDD Results are the same")
        } else {
          println("EDD Results differ:")
          println(log)
        }
        true
      }
      .orElse(Some(false))()
  }

  /**
   * execute as dry-run if the dry-run flag is specified.
   * This will show which modules are not yet persisted that need to run, without
   * actually running the modules.
   * @return true if dry-run option was specified, otherwise false
   */
  private def dryRun(): Boolean = {
    if (smvConfig.cmdLine.dryRun()) {

      // find all ancestors inclusive, and in case of SmvModuleLink, resolve its target module.
      // filter the modules that are not yet persisted and not ephemeral.
      // this yields all the modules that will need to be run with the given command
      val modsNotPersisted = modulesToRun.flatMap( m =>
        m +: m.ancestors
      ).map(_ match {
          case l: SmvModuleLink => l.smvModule
          case m => m
        }
      ).filterNot(m =>
        m.isPersisted || m.isEphemeral
      ).distinct

      println("Dry run - modules not persisted:")
      println("----------------------")
      println(modsNotPersisted.mkString("\n"))
      println("----------------------")
      true
    } else {
      false
    }
  }

  /**
   * if the publish to hive flag is setn, the publish
   */
  def publishModulesToHive(collector: SmvRunInfoCollector): Boolean = {
    if (publishHive) {
      // filter out the outout modules and publish them
      modulesToRun flatMap {
        case m: SmvOutput => Some(m)
        case _            => None
      } foreach (
          m => m.exportToHive(collector)
      )
    }

    publishHive
  }

  /**
   * if the export-csv option is specified, then publish locally
   */
  def publishOutputModulesLocally(collector: SmvRunInfoCollector): Boolean = {
    if (smvConfig.cmdLine.exportCsv.isSupplied) {
      val localDir = smvConfig.cmdLine.exportCsv()
      modulesToRun foreach { m =>
        val csvPath = s"${localDir}/${m.versionedFqn}.csv"
        m.rdd(collector=collector).smvExportCsv(csvPath)
      }
    }

    smvConfig.cmdLine.exportCsv.isSupplied
  }

  /**
   * Publish through JDBC if the --publish-jdbc flag is set
   */
  def publishOutputModulesThroughJDBC(collector: SmvRunInfoCollector): Boolean = {
    if (publishJDBC) {
      modulesToRun foreach (_.publishThroughJDBC(collector))
      true
    } else {
      false
    }
  }

  /**
   * Publish the specified modules if the "--publish" flag was specified on command line.
   * @return true if modules were published, otherwise return false.
   */
  private def publishOutputModules(collector: SmvRunInfoCollector): Boolean = {
    if (smvConfig.cmdLine.publish.isDefined) {
      modulesToRun foreach { module =>
        module.publish(collector=collector)
      }
      true
    } else {
      false
    }
  }

  /**
   * run the specified output modules.
   * @return true if modules were generated, otherwise false.
   */
  private def generateOutputModules(collector: SmvRunInfoCollector): Boolean = {
    modulesToRun foreach (_.rdd(collector=collector))
    modulesToRun.nonEmpty
  }

  /**
   * set dynamic runtime configuration.
   * this should be set before run dataset.
   */
  private def setDynamicRunConfig(runConfig: Map[String, String]) = {
    smvConfig.dynamicRunConfig = runConfig
  }

  /**
   * proceeds with the execution of an smvDS passed from runModule or runModuleByName
   * TODO: the name of this function should make its distinction from runModule clear (this is an implementation)
   */
  private def runDS(ds: SmvDataSet,
            forceRun: Boolean,
            version: Option[String],
            collector: SmvRunInfoCollector): DataFrame = {
    if (version.isDefined)
      // if fails, error already handled since input path doesn't exist
      ds.readPublishedData(version).get
    else {
      if (forceRun)
        deletePersistedResults(Seq(ds))
      ds.rdd(forceRun, collector=collector)
    }
  }

  /** Run a module by its fully qualified name in its respective language environment
   *  If force argument is true, any existing persisted results will be deleted
   *  and the module's DataFrame cache will be ignored, forcing the module to run again.
   *  If a version is specified, try to read the module from the published data for the given version.
   *  If dynamic runtime configuration is specified, run the module with the configuration provided.
   */
  def runModule(urn: URN,
                forceRun: Boolean = false,
                version: Option[String] = None,
                runConfig: Map[String, String] = Map.empty,
                collector: SmvRunInfoCollector = new SmvRunInfoCollector): DataFrame = {
    // set dynamic runtime configuration before discovering ds as stage, etc impacts what can be discovered
    setDynamicRunConfig(runConfig)
    val ds = dsm.load(urn).head
    runDS(ds, forceRun, version, collector)
  }

  /**
   * Run a module based on the end of its name (must be unique). If force argument
   * is true, any existing persisted results will be deleted and the module's
   *  DataFrame cache will be ignored, forcing the module to run again.
   * If a version is specified, try to read the module from the published data for the given version
   */
  def runModuleByName(modName: String,
                      forceRun: Boolean = false,
                      version: Option[String] = None,
                      runConfig: Map[String, String] = Map.empty,
                      collector: SmvRunInfoCollector = new SmvRunInfoCollector): DataFrame = {
    // set dynamic runtime configuration before discovering ds as stage, etc impacts what can be discovered
    setDynamicRunConfig(runConfig)
    val ds = dsm.inferDS(modName).head

    runDS(ds, forceRun, version, collector=collector)
  }

  def publishModuleToHiveByName(modName: String,
                                runConfig: Map[String, String],
                                collector: SmvRunInfoCollector): Unit = {
      setDynamicRunConfig(runConfig)
      dsm.inferDS(modName).head.exportToHive(collector)
  }

  def getDsHash(name: String, runConfig: Map[String, String]): String = {
    setDynamicRunConfig(runConfig)
    dsm.inferDS(name).head.verHex
  }

  def getRunInfo(partialName: String, runConfig: Map[String, String]): SmvRunInfoCollector = {
    setDynamicRunConfig(runConfig)
    getRunInfo(dsm.inferDS(partialName).head)
  }

  def getRunInfo(urn: URN, runConfig: Map[String, String]): SmvRunInfoCollector = {
    setDynamicRunConfig(runConfig)
    getRunInfo(dsm.load(urn).head)
  }

  /**
   * Returns the run information for a given dataset and all its
   * dependencies (including transitive dependencies), from the last run
   */
  def getRunInfo(ds: SmvDataSet,
    coll: SmvRunInfoCollector=new SmvRunInfoCollector()): SmvRunInfoCollector = {
    // get fqn from urn, because if ds is a link we want the fqn of its target
    coll.addRunInfo(ds.fqn, ds.runInfo)

    ds.resolvedRequiresDS foreach { dep =>
      val depTarget = dep match {
        case link: SmvModuleLink => link.smvModule
        case _                   => dep
      }
      getRunInfo(depTarget, coll)
    }

    coll
  }

  /**
   * Returns metadata for a given urn
   */
  def getMetadataJson(urn: URN): String = {
    val ds = dsm.load(urn).head
    ds.getMetadata().toJson
  }

  /**
   * sequence of SmvModules to run based on the command line arguments.
   * Returns the union of -a/-m/-s command line flags.
   */
  lazy val modulesToRun: Seq[SmvDataSet] = {
    val cmdline = smvConfig.cmdLine
    val empty   = Some(Seq.empty[String])

    val modPartialNames = cmdline.modsToRun.orElse(empty)()
    val stageNames      = cmdline.stagesToRun.orElse(empty)() map (dsm.inferStageFullName(_))

    dsm.modulesToRun(modPartialNames, stageNames, cmdline.runAllApp())
  }

  /**
   * Sequence of SmvModules to run + all of their ancestors
   */
  lazy val modulesToRunWithAncestors: Seq[SmvDataSet] = {
    val ancestors = modulesToRun flatMap (_.ancestors)
    (modulesToRun ++ ancestors).distinct
  }

  /**
   * The main entry point into the app.  This will parse the command line arguments
   * to determine which modules should be run/graphed/etc.
   */
  def run() = {
    purgeCurrentOutputFiles()
    purgeOldOutputFiles()

    val mods = modulesToRun

    if (mods.nonEmpty) {
      println("Modules to run/publish")
      println("----------------------")
      println(mods.map(_.fqn).mkString("\n"))
      println("----------------------")
    }

    val collector = new SmvRunInfoCollector

    // either generate graphs, publish modules, or run output modules (only one will occur)
    printDeadModules || dryRun() || compareEddResults() ||
      generateDotDependencyGraph() || generateJsonDependencyGraph() ||
      publishModulesToHive(collector) ||  publishOutputModules(collector) ||
      publishOutputModulesThroughJDBC(collector) || publishOutputModulesLocally(collector) ||
      generateOutputModules(collector)
  }
}

/**
 * Common entry point for all SMV applications.  This is the object that should be provided to spark-submit.
 */
object SmvApp {
  var app: SmvApp = _

  def init(args: Array[String],
           _sc: Option[SparkContext] = None,
           _sql: Option[SQLContext] = None) = {
    app = new SmvApp(args, _sc, _sql)
    app
  }

  /**
   * Creates a new app instances from a sql context.  This is used by SparkR to create a new app.
   */
  def newApp(sqlContext: SQLContext, appPath: String): SmvApp = {
    SmvApp.init(Seq("-m", "None", "--smv-app-dir", appPath).toArray,
                Option(sqlContext.sparkContext),
                Option(sqlContext))
    SmvApp.app
  }

  def main(args: Array[String]) {
    init(args)
    app.run()
  }
}
