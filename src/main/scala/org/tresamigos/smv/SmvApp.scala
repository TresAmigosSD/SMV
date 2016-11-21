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

/**
 * Driver for SMV applications.  Most apps do not need to override this class and should just be
 * launched using the SmvApp object (defined below)
 */
class SmvApp (private val cmdLineArgs: Seq[String], _sc: Option[SparkContext] = None, _sql: Option[SQLContext] = None) {

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

  /** Dataframes from resolved modules */
  private[smv] var dataframes: Map[String, DataFrame] = Map.empty

  val scalaDataSets = new ScalaDataSetRepository

  // Since OldVersionHelper will be used by executors, need to inject the version from the driver
  OldVersionHelper.version = sc.version

  // configure spark sql params and inject app here rather in run method so that it would be done even if we use the shell.
  setSparkSqlConfigParams()

  /** stack of items currently being resolved.  Used for cyclic checks. */
  val resolveStack: mutable.Stack[String] = mutable.Stack()

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

  /** The names of output modules in a given stage */
  def outputModsForStage(stageName: String, repos: SmvDataSetRepository*): Seq[String] =
    if (repos.isEmpty) outputModsForStage(stageName, scalaDataSets) else
      repos.flatMap(_.outputModsForStage(stageName).split(","))

  /** Output modules */
  def modulesToRun(repos: SmvDataSetRepository*): Seq[String] = {
    val cl = smvConfig.cmdLine
    val directMods: Seq[String] = cl.modsToRun()
    val stageMods: Seq[String] = cl.stagesToRun().flatMap(outputModsForStage(_, repos:_*))
    val appMods: Seq[String] =
      if (cl.runAllApp()) stages.stageNames.flatMap(outputModsForStage(_, repos:_*)) else Nil

    (directMods ++ stageMods ++ appMods).filterNot(_.isEmpty)
  }

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
  def checkDependencyRules(ds: SmvDataSet): Seq[DependencyViolation] =
    if (ds.isInstanceOf[SmvExtDataSet]) Seq.empty else {
      val results = SmvApp.DependencyRules map (_.check(ds))
      results.foldRight(Seq.empty[DependencyViolation])((elem, acc) => elem.toSeq ++: acc)
    }

  /** Textual representation for output to console */
  def mkViolationString(violations: Seq[DependencyViolation], indent: String = ".."): String =
    (for {
      v <- violations
      header = s"${indent}${v.description}"
    } yield
      (header +: v.components.map(m => s"${indent}${indent}${m.name}")).mkString("\n")
    ).mkString("\n")

  /**
   * Get the RDD associated with data set.  The rdd plan (not data) is cached in the SmvDataSet
   * to ensure only a single DataFrame exists for a given data set (file/module).
   * The module can create a data cache itself and the cached data will be used by all
   * other modules that depend on the required module.
   * This method also checks for cycles in the module dependency graph.
   */
  def resolveRDD(ds: SmvDataSet): DataFrame = {
    val dsName = ds.name
    if (resolveStack.contains(dsName))
      throw new IllegalStateException(s"cycle found while resolving ${dsName}: " +
        resolveStack.mkString(","))

    resolveStack.push(dsName)

    /* ds.rdd will trigger resolveRDD on all the DataSets current one depends on, which
       will push them all to the stack.
       In Spark shell, when ds.rdd fails, dsName is still in the stack, need to pop it
       so that redefining the same ds will not cause a "cycle" error */
    val resRdd = try {
      val violations = checkDependencyRules(ds)
      if (!violations.isEmpty) {
        println(s"""Module ${ds.name} violates dependency rules""")
        println(mkViolationString(violations))

        if (!smvConfig.permitDependencyViolation)
          throw new IllegalStateException(s"Terminating module resolution when dependency rules are violated.  To change this behavior, please run the app with option --${smvConfig.cmdLine.permitDependencyViolation.name}")
        else
          println("Continuing module resolution as the app is to configured to permit dependency rule violation")
      }

      ds.rdd()
    } catch {
      case NonFatal(t) => {
        resolveStack.pop()
        throw t
      }
    }

    val popRdd = resolveStack.pop()
    if (popRdd != dsName)
      throw new IllegalStateException(s"resolveStack corrupted.  Got ${popRdd}, expected ${dsName}")

    resRdd
  }

  /**
   * dynamically resolve a module.
   * The module and all its dependents are loaded into their own classloader so that we can have multiple
   * instances of the same module loaded at different times.
   */
  def dynamicResolveRDD(fqn: String, parentClassLoader: ClassLoader) =
    resolveRDD(dsForName(fqn, parentClassLoader))

  /** Looks up an SmvDataSet by its fully-qualified name */
  def dsForName(fqn: String, parentClassLoader: ClassLoader) = {
    val cl = SmvClassLoader(smvConfig, parentClassLoader)
    val ref = new SmvReflection(cl)
    ref.objectNameToInstance[SmvDataSet](fqn)
  }

  lazy val packagesPrefix = {
    val m = allAppModules
    if (m.isEmpty) ""
    else m.map(_.name).reduce{(l,r) =>
        (l.split('.') zip r.split('.')).
          collect{ case (a, b) if (a==b) => a}.mkString(".")
      } + "."
  }

  /** clean name in graph output */
  private[smv] def moduleNameForPrint(ds: SmvDataSet) = ds.name.stripPrefix(packagesPrefix)

  private def genDotGraph(module: SmvModule) = {
    val pathName = s"${module.name}.dot"
    val graphString = new graph.SmvGraphUtil(stages).createGraphvisCode(Seq(module))
    SmvReportIO.saveLocalReport(graphString, pathName)
  }

  def genJSON(packages: Seq[String] = Seq()) = {
    val pathName = s"${smvConfig.appName}.json"

    val stagesToGraph = packages.map{stages.findStage(_)}
    val graphString = new graph.SmvGraphUtil(new SmvStages(stagesToGraph)).createGraphJSON()

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
    smvConfig.modulesToRun().foreach {m => m.deleteOutputs()}
  }

  /**
   * generate dependency graphs if "-g" flag was specified on command line.
   * @return true of graphs were generated otherwise return false.
   */
  private def generateDependencyGraphs() : Boolean = {
    if (smvConfig.cmdLine.graph()) {
      smvConfig.modulesToRun().foreach { module =>
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
      smvConfig.modulesToRun().foreach { module => module.publish() }
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

    smvConfig.modulesToRun().foreach { module =>
      val modResult = resolveRDD(module)

      // if module was ephemeral, then it was not saved during graph execution and we need
      // to persist it here explicitly.
      if (module.isEphemeral)
        module.persist(modResult)
    }

    smvConfig.modulesToRun().nonEmpty
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
  def runModule(modfqn: String, repos: SmvDataSetRepository*): DataFrame =
    if (modfqn.isEmpty)
      return null
    else if (dataframes.contains(modfqn))
      dataframes(modfqn)
    else if (repos.isEmpty)
      runModule(modfqn, scalaDataSets)
    else if (resolveStack contains modfqn)
      throw new IllegalStateException(s"cycle found while resolving ${modfqn}: " + resolveStack.mkString(","))
    else {
      println(s"running module ${modfqn}")
      resolveStack.push(modfqn)
      repos.find(_.hasDataSet(modfqn)) match {
        case None =>
          resolveStack.pop()
          throw new SmvRuntimeException(s"Cannot find module [${modfqn}]")
        case Some(repo) =>
          dependencies(repo, modfqn) foreach (runModule(_, repos:_*))

          val df = try {
            if (repo.isExternal(modfqn)) {
              runModule(repo.getExternalDsName(modfqn), repos:_*)
            }
            else if (repo.isLink(modfqn)) {
              val targetName = repo.getLinkTargetName(modfqn)
              stages.inferStageNameFromDsName(modfqn) flatMap { stageVersion =>
                SmvUtil.readPersistedFile(sqlContext, publishPath(targetName, stageVersion), null).toOption
              } getOrElse runModule(targetName, repos:_*)
            }
            else {
              val dqm = new DQMValidator(repo.getDqm(modfqn))
              persist(repo, modfqn, dqm, repo.getDataFrame(modfqn, dqm, dataframes))
            }
          } finally {
            resolveStack.pop()
          }

          // dataframe from external modules would be registered under
          // both its implementing module name and the referenced name
          dataframes = dataframes + (modfqn -> df)
          df
      }
    }

  // helper method till we can return the Seq directly from Python implementation
  private def dependencies(repo: SmvDataSetRepository, modfqn: String): Seq[String] =
    repo.dependencies(modfqn).split(',').filterNot(_.isEmpty)

  // TODO: handle external dependencies
  def hashOfHash(repo: SmvDataSetRepository, modfqn: String): Int =
    dependencies(repo, modfqn).foldLeft(
      repo.datasetHash(modfqn, true)) { (acc, dep) =>
      acc + hashOfHash(repo, dep)
    }.toInt

  private def persist(repo: SmvDataSetRepository, modfqn: String, dqm: DQMValidator, df: DataFrame): DataFrame = {
    val hashval: Int = hashOfHash(repo, modfqn)
    // modules defined in spark shell starts with '$'
    val persistValidation = !modfqn.startsWith("$")
    val validator = new ValidationSet(Seq(dqm), persistValidation)
    if (repo.isEphemeral(modfqn)) {
      val r = dqm.attachTasks(df)
      // no action before this  point
      validator.validate(r, false, moduleValidPath(modfqn, hashval))
      r
    } else {
      val path = moduleCsvPath(modfqn, hashval)
      SmvUtil.readPersistedFile(sqlContext, path).recoverWith { case ex =>
        val r = dqm.attachTasks(df)
        SmvUtil.persist(sqlContext, r, path, genEdd)
        // already had action from persist
        validator.validate(r, true, moduleValidPath(modfqn, hashval))
        SmvUtil.readPersistedFile(sqlContext, path)
      }.get
    }
  }

  /**
   * Discard the cached result of the module, if any, and re-run.
   *
   * This is usually used in an interactive shell to re-run a module after it's been modified.
   */
  def runDynamicModule(modfqn: String, repos: SmvDataSetRepository*): DataFrame =
    if (modfqn.isEmpty)
      return null
    else if (repos.isEmpty)
      runDynamicModule(modfqn, scalaDataSets)
    else if (!dataframes.contains(modfqn))
      runModule(modfqn, repos:_*)
    else
      repos.find(_.hasDataSet(modfqn)) match {
        case None =>
          throw new SmvRuntimeException("")
        case Some(repo) =>
          dependencies(repo, modfqn) foreach (runDynamicModule(_, repos:_*))

          dataframes = dataframes - modfqn
          val df =
            if (repo.isExternal(modfqn))
              runDynamicModule(repo.getExternalDsName(modfqn), repos:_*)
            else {
              val dqm = new DQMValidator(repo.getDqm(modfqn))
              persist(repo, modfqn, dqm, repo.rerun(modfqn, dqm, dataframes))
            }
          dataframes = dataframes + (modfqn -> df)

          df
      }

  /**
   * Publish the result of an SmvModule
   */
  def publishModule(modfqn: String, version: String, repos: SmvDataSetRepository*): Unit = {
    val df = runModule(modfqn, repos:_*)
    val path = publishPath(modfqn, version)
    SmvUtil.publish(sqlContext, df, path, genEdd)
  }

  /**
   * Run a module given it's name.  This is mostly used by SparkR to resolve modules.
   */
  def runModuleByName(modName: String) : DataFrame = {
    val module = smvConfig.resolveModuleByName(modName)
    resolveRDD(module)
  }

  /**
   * Dynamically load a module given FQN. This is mostly used by SparkR to dynamically
   * resolve modules
   **/
  def runDynamicModuleByName(fqn: String): DataFrame = {
    val cl = getClass.getClassLoader
    dynamicResolveRDD(fqn: String, cl)
  }

  /**
   * The main entry point into the app.  This will parse the command line arguments
   * to determine which modules should be run/graphed/etc.
   */
  def run() = {
    val mods = smvConfig.modulesToRun
    if (mods.nonEmpty) {
      println("Modules to run/publish")
      println("----------------------")
      println(mods.map(_.name).mkString("\n"))
      println("----------------------")

      println()
      println("..checking dependency rules for all modules")
      val all = (mods ++ mods.flatMap(_.dependencies)).distinct
      all.foreach { m =>
        val violations = checkDependencyRules(m)
        if (violations.isEmpty)
          println(s"..module ${m.name} .... pass")
        else {
          println(s"..module ${m.name} violates dependency rules ... FAIL")
          println(mkViolationString(violations, "...."))
        }
      }
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
