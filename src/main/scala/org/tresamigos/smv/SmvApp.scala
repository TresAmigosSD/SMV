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

import java.io.{File, PrintWriter}
import org.rogach.scallop.ScallopConf

import scala.reflect.runtime.{universe => ru}
import org.apache.spark.sql.{SchemaRDD, SQLContext}
import org.apache.spark.{SparkContext, SparkConf}

import scala.collection.mutable
import scala.util.Try
import java.util.Calendar

/**
 * Dependency management unit within the SMV application framework.  Execution order within
 * the SMV application framework is derived from dependency between SmvDataSet instances.
 * Instances of this class can either be a file or a module. In either case, there would
 * be a single result SchemaRDD.
 */
abstract class SmvDataSet {

  private var rddCache: SchemaRDD = null
  private[smv] var versionSumCache : Int = -1

  def name(): String
  def description(): String

  /** concrete classes must implement this to supply the uncached RDD for data set */
  def computeRDD(app: SmvApp): SchemaRDD

  /** modules must override to provide set of datasets they depend on. */
  def requiresDS() : Seq[SmvDataSet]

  /** code "version".  Derived classes should update the value when code or data */
  def version() : Int = 0
  
  /**
    * the version of this dataset plus the versions of all dependent data sets.
    * Need to cache the computed value to avoid a O(n!) algorithm.
    */
  private[smv] def versionSum() : Int = {
    if (versionSumCache < 0)
      versionSumCache = requiresDS().map(_.versionSum).sum + version
    versionSumCache
  }

  /**
   * returns the SchemaRDD from this dataset (file/module).
   * The value is cached so this function can be called repeatedly.
   */
  def rdd(app: SmvApp) = {
    if (rddCache == null)
      rddCache = computeRDD(app)
    rddCache
  }
}

abstract class SmvFile extends SmvDataSet{
  val basePath: String
  override def description() = s"Input file: @${basePath}"
  override def requiresDS() = Seq.empty
  
  override def name() = {
    val nameRex = """(.+)(.csv)*(.gz)*""".r
    basePath match {
      case nameRex(v, _, _) => v
      case _ => throw new IllegalArgumentException(s"Illegal base path format: $basePath")
    }
  }
}

/**
 * Represents a raw input file with a given file path (can be local or hdfs) and CSV attributes.
 */
case class SmvCsvFile(basePath: String, csvAttributes: CsvAttributes) extends
  SmvFile{

  override def computeRDD(app: SmvApp): SchemaRDD = {
    implicit val ca = csvAttributes
    implicit val rejectLogger = app.rejectLogger
    app.sqlContext.csvFileWithSchema(s"${app.dataDir}/${basePath}")
  }
  
}

case class SmvFrlFile(basePath: String) extends
    SmvFile{
      
  override def computeRDD(app: SmvApp): SchemaRDD = {
    implicit val rejectLogger = app.rejectLogger
    app.sqlContext.frlFileWithSchema(s"${app.dataDir}/${basePath}")
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
 * result of the run is the result of the module.  All modules that depend on this module
 * will be provided the SchemaRDD result from the run method of this module.
 * Note: the module should *not* persist any RDD itself.
 */
abstract class SmvModule(val description: String) extends SmvDataSet {

  override val name = this.getClass().getName().filterNot(_=='$')

  type runParams = Map[SmvDataSet, SchemaRDD]
  def run(inputs: runParams) : SchemaRDD

  /** perform the actual run of this module to get the generated SRDD result. */
  private def doRun(app: SmvApp): SchemaRDD = {
    run(requiresDS().map(r => (r, app.resolveRDD(r))).toMap)
  }

  private def fullPath(app: SmvApp) = {
    val prefix = s"${app.dataDir}/output/${name}"
    if (app.isDevMode) {
      s"${prefix}_${versionSum()}.csv"
    } else {
      s"${prefix}.csv"
    }
  }

  private[smv] def persist(app: SmvApp, rdd: SchemaRDD) = {
    val filePath = fullPath(app)
    implicit val ca = CsvAttributes.defaultCsvWithHeader
    val fmtObj = new java.text.SimpleDateFormat("HH:mm:ss")
    if (app.isDevMode){
      val c = new ScCounter(app.sc)
      var now = fmtObj.format(java.util.Calendar.getInstance().getTime())
      println(s"${now} PERSISTING: ${filePath}")
      rdd.pipeCount(c).saveAsCsvWithSchema(filePath)
      now = fmtObj.format(java.util.Calendar.getInstance().getTime())
      println(s"${now} Number of records persisted: " + c("N"))
    } else {
      rdd.saveAsCsvWithSchema(filePath)
    }
  }

  private[smv] def readPersistedFile(app: SmvApp): Try[SchemaRDD] = {
    implicit val ca = CsvAttributes.defaultCsvWithHeader
    Try(app.sqlContext.csvFileWithSchema(fullPath(app)))
  }

  override def computeRDD(app: SmvApp): SchemaRDD = {
    if (app.isDevMode) {
      readPersistedFile(app).recoverWith { case e =>
        // if unable to read persisted file, recover by running the module and persist.
        persist(app, doRun(app))
        readPersistedFile(app)
      }.get
    } else {
      doRun(app)
    }
  }
}

/**
 * Driver for SMV applications.  The app may override the getModulesPackages method to
 * provide list of modules to discover SmvModules (only needed for catalogs).
 */
abstract class SmvApp (val appName: String, private val cmdLineArgs: Seq[String], _sc: Option[SparkContext] = None) {

  val cmdLineArgsConf = new CmdLineArgsConf(cmdLineArgs)
  val isDevMode = cmdLineArgsConf.devMode()
  val sparkConf = new SparkConf().setAppName(appName)
  val sc = _sc.getOrElse(new SparkContext(sparkConf))
  val sqlContext = new SQLContext(sc)
  private val mirror = ru.runtimeMirror(this.getClass.getClassLoader)

  /** stack of items currently being resolved.  Used for cyclic checks. */
  val resolveStack: mutable.Stack[String] = mutable.Stack()

  /** concrete applications can provide a list of package names containing modules. */
  def getModulePackages() : Seq[String] = Seq.empty

  /** concrete applications can provide a more interesting RejectLogger. 
   *  Example: override val rejectLogger = new SCRejectLogger(sc, 3)
   */
  def rejectLogger() : RejectLogger = TerminateRejectLogger
  
  def saveRejects(path: String) = {
    if(rejectLogger.isInstanceOf[SCRejectLogger]){
      val r = rejectLogger.rejectedReport
      if(!r.isEmpty){
        sc.makeRDD(r, 1).saveAsTextFile(path)
        println(s"RejectLogger is not empty, please check ${path}")
      }
    }
  }
  
  private[smv] val dataDir = sys.env.getOrElse("DATA_DIR", "/DATA_DIR_ENV_NOT_SET")

  /**
   * Get the RDD associated with data set.  The rdd plan (not data) is cached in the SmvDataSet
   * to ensure only a single SchemaRDD exists for a given data set (file/module).
   * The module can create a data cache itself and the cached data will be used by all
   * other modules that depend on the required module.
   * This method also checks for cycles in the module dependency graph.
   */
  def resolveRDD(ds: SmvDataSet): SchemaRDD = {
    val dsName = ds.name
    if (resolveStack.contains(dsName))
      throw new IllegalStateException(s"cycle found while resolving ${dsName}: " +
        resolveStack.mkString(","))

    resolveStack.push(dsName)

    val resRdd = ds.rdd(this)

    val popRdd = resolveStack.pop()
    if (popRdd != dsName)
      throw new IllegalStateException(s"resolveStack corrupted.  Got ${popRdd}, expected ${dsName}")

    resRdd
  }

  /** maps the FQN of module name to the module object instance. */
  private[smv] def moduleNametoObject(modName: String) = {
    mirror.reflectModule(mirror.staticModule(modName)).instance.asInstanceOf[SmvModule]
  }


  /** extract instances (objects) in given package that implement SmvModule. */
  private[smv] def modulesInPackage(pkgName: String): Seq[SmvModule] = {
    import com.google.common.reflect.ClassPath
    import scala.collection.JavaConversions._

    ClassPath.from(this.getClass.getClassLoader).
      getTopLevelClasses(pkgName).
      map(c => Try(moduleNametoObject(c.getName))).
      filter(_.isSuccess).
      map(_.get).
      toSeq
  }

  /** find a common prefix to all module names. */
  /* we should be more functional than this :-) 
  private def findCommonPrefix(prefix: String, allModuleNames: Seq[String]): String = {
    if (prefix.size == 0)
      ""
    else
      if (allModuleNames.forall(m => m.startsWith(prefix)))
        prefix
      else
        findCommonPrefix(prefix.split('.').dropRight(1).mkString("."), allModuleNames)
  }
  */

  def allModules() = getModulePackages.map(modulesInPackage).flatten
  def packagesPrefix() = {
    val m = allModules()
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
    new SmvModuleDependencyGraph(module, packagesPrefix).saveToFile(pathName)
  }
  
  def genJSON(packages: Seq[String] = Seq()) = {
    val pathName = s"${appName}.json"
    new SmvModuleJSON(this, packages).saveToFile(pathName)
  }

  /**
   * The main entry point into the app.  This will parse the command line arguments
   * to determine which modules should be run/graphed/etc.
   */
  def run() = {
    if (cmdLineArgsConf.json()) {
      genJSON();
    }

    cmdLineArgsConf.modules().foreach { module =>
      val modObject = moduleNametoObject(module)

      if (cmdLineArgsConf.graph()) {
        // TODO: need to combine the modules for graphs into a single graph.
        genDotGraph(modObject)
      } else {
        val modResult = resolveRDD(modObject)

        if (! isDevMode)
          modObject.persist(this, modResult)
      }
    }
  }
}

/**
 * command line argumetn parsing using scallop library.
 * See (https://github.com/scallop/scallop) for details on using scallop.
 */
private[smv] class CmdLineArgsConf(args: Seq[String]) extends ScallopConf(args) {
  val devMode = toggle("dev", default=Some(false),
    descrYes="enable dev mode (persist all intermediate module results",
    descrNo="enable production mode (all modules are evaluated from scratch")
  val graph = toggle("graph", default=Some(false),
    descrYes="generate a dependency graph of the given modules (modules are not run)",
    descrNo="do not generate a dependency graph")
  val json = toggle("json", default=Some(false),
    descrYes="generate a json object to represent entire app's module dependency (modules are not run)",
    descrNo="do not generate a json")
  val modules = trailArg[List[String]](descr="FQN of modules to run/graph")
}

private[smv] class SmvModuleJSON(app: SmvApp, packages: Seq[String]) {
  private def allModules = {
    if (packages.isEmpty) app.allModules
    else packages.map{app.packagesPrefix + _}.map(app.modulesInPackage).flatten
  }.sortWith{(a,b) => a.name < b.name}
  
  private def allFiles = allModules.flatMap(m => m.requiresDS().filter(v => v.isInstanceOf[SmvFile]))

  private def printName(m: SmvDataSet) = m.name.stripPrefix(app.packagesPrefix) 

  def generateJSON() = {
    "{\n" +
    allModules.map{m => 
      s"""  "${printName(m)}": {""" + "\n" +
      s"""    "version": ${m.versionSum()},""" + "\n" +
      s"""    "dependents": [""" + m.requiresDS().map{v => s""""${printName(v)}""""}.mkString(",") + "],\n" +
      s"""    "description": "${m.description}"""" + "}" 
    }.mkString(",\n") +
    "}"
  }

  def saveToFile(filePath: String) = {
    val pw = new PrintWriter(new File(filePath))
    pw.println(generateJSON())
    pw.close()
  }
}
/**
 * contains the module level dependency graph starting at the given startNode.
 * All prefixes given in packagePrefixes are removed from the output module/file name
 * to make the graph cleaner.  For example, project X at com.foo should probably add
 * a package prefix of "com.foo.X" to remove the repeated noise of "com.foo.X" before
 * every module name in the graph.
 */
private[smv] class SmvModuleDependencyGraph(val startMod: SmvModule, packagesPrefix: String) {
  type dependencyMap = Map[SmvDataSet, Seq[SmvDataSet]]

  private def addDependencyEdges(node: SmvDataSet, nodeDeps: Seq[SmvDataSet], map: dependencyMap): dependencyMap = {
    if (map.contains(node)) {
      map
    } else {
      nodeDeps.foldLeft(map.updated(node, nodeDeps))(
        (curMap,child) => addDependencyEdges(child, child.requiresDS(), curMap))
    }
  }

  private[smv] lazy val graph = {
    addDependencyEdges(startMod, startMod.requiresDS(), Map())
  }

  private lazy val allFiles = graph.values.flatMap(vs => vs.filter(v => v.isInstanceOf[SmvFile])).toSet.toSeq
  private lazy val allModules = graph.flatMap(kv => (Seq(kv._1) ++ kv._2).filter(v => v.isInstanceOf[SmvModule])).toSet.toSeq

  private def printName(m: SmvDataSet) = m.name.stripPrefix(packagesPrefix) 
  /** quoted/clean name in graph output */
  private def q(ds: SmvDataSet) = "\"" + printName(ds) + "\""

  private def moduleStyles() = {
    allModules.map(m => s"  ${q(m)} " + "[tooltip=\"" + s"${m.description}" + "\"]")
  }

  private def fileStyles() = {
    allFiles.map(f => s"  ${q(f)} " + "[shape=box, color=\"pink\"]")
  }

  private def filesRank() = {
    Seq("{ rank = same; " + allFiles.map(f => s"${q(f)};").mkString(" ") + " }")
  }

  private def generateGraphvisCode() = {
    Seq(
      "digraph G {",
      "  rankdir=\"TB\";",
      "  node [style=filled,color=\"lightblue\"]") ++
      fileStyles() ++
      filesRank() ++
      moduleStyles() ++
      graph.flatMap{case (k,vs) => vs.map(v => s"""  ${q(v)} -> ${q(k)} """ )} ++
      Seq("}")
  }

  def saveToFile(filePath: String) = {
    val pw = new PrintWriter(new File(filePath))
    generateGraphvisCode().foreach(pw.println)
    pw.close()
  }
}

