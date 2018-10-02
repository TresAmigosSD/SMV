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


import collection.JavaConverters._
import java.util.List
import scala.collection.mutable
import scala.io.Source
import scala.util.{Try, Success, Failure}

import org.apache.log4j.LogManager
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.{SparkContext, SparkConf}
import org.joda.time.DateTime

import org.tresamigos.smv.util.Edd


/**
 * Driver for SMV applications.  Most apps do not need to override this class and should just be
 * launched using the SmvApp object (defined below)
 */
class SmvApp(private val cmdLineArgs: Seq[String], _spark: SparkSession) {
  val log         = LogManager.getLogger("smv")
  val smvConfig   = new SmvConfig(cmdLineArgs)
  val genEdd      = smvConfig.cmdLine.genEdd()
  val publishHive = smvConfig.cmdLine.publishHive()
  val publishJDBC = smvConfig.cmdLine.publishJDBC()

  def stages      = smvConfig.stageNames
  def userLibs    = smvConfig.userLibs

  lazy val smvVersion  = {
    val smvHome = sys.env("SMV_HOME")
    val versionFile = Source.fromFile(f"${smvHome}/.smv_version")
    val nextLine = versionFile.getLines.next
    versionFile.close
    nextLine
  }

  val sparkSession = _spark 

  val sc         = sparkSession.sparkContext
  val sqlContext = sparkSession.sqlContext

  // dsm should be private but will be temporarily public to accomodate outside invocations
  val dsm = new DataSetMgr(smvConfig)

  // Since OldVersionHelper will be used by executors, need to inject the version from the driver
  OldVersionHelper.version = sc.version

  // configure spark sql params and inject app here rather in run method so that it would be done even if we use the shell.
  setSparkSqlConfigParams()

  lazy val allDataSets = dsm.allDataSets

  /** list of all current valid output files in the output directory. All other files in output dir can be purged. */
  private[smv] def validFilesInOutputDir(): Seq[String] =
    allDataSets.flatMap(_.allOutputFiles).map(SmvHDFS.baseName(_))

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
  private[smv] def generateDotDependencyGraph() : Boolean = {
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
  private[smv] def generateJsonDependencyGraph() : Boolean = {
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
  private[smv] def compareEddResults(): Boolean = {
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
  private[smv] def publishOutputModules(collector: SmvRunInfoCollector): Boolean = {
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
  private[smv] def generateOutputModules(collector: SmvRunInfoCollector): Boolean = {
    modulesToRun foreach (_.rdd(collector=collector))
    modulesToRun.nonEmpty
  }

  def getRunInfo(urn: URN): SmvRunInfoCollector = {
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
      getRunInfo(dep, coll)
    }

    coll
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
}

/**
 * Common entry point for all SMV applications.  This is the object that should be provided to spark-submit.
 */
object SmvApp {
  var app: SmvApp = _

  def init(args: Array[String], _spark: SparkSession) = {
    app = new SmvApp(args, _spark)
    app
  }

  /**
   * Creates a new app instances from a sql context.  This is used by SparkR to create a new app.
   */
  def newApp(sparkSession: SparkSession, appPath: String): SmvApp = {
    SmvApp.init(Seq("-m", "None", "--smv-app-dir", appPath).toArray, sparkSession)
    SmvApp.app
  }
}
