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
import scala.reflect.runtime.{universe => ru}
import org.apache.spark.sql.{SchemaRDD, SQLContext}
import org.apache.spark.{SparkContext, SparkConf}

import scala.collection.mutable

/**
 * Dependency management unit within the SMV application framework.  Execution order within
 * the SMV application framework is derived from dependency between SmvDataSet instances.
 * Instances of this class can either be a file or a module. In either case, there would
 * be a single result SchemaRDD.
 */
abstract class SmvDataSet(val description: String) {

  def name(): String

  // dataDir should really be at the app level
  private[smv] val dataDir = sys.env.getOrElse("DATA_DIR", "/DATA_DIR_ENV_NOT_SET")

  /** full path of file associated with this module/file */
  def fullPath(): String

  /** returns the SchemaRDD from this dataset (file/module) */
  def rdd(app: SmvApp): SchemaRDD

  /** modules must override to provide set of datasets they depend on. */
  def requiresDS() : Seq[SmvDataSet]

  def allRequireNames() = requiresDS().map(_.name)
}

/**
 * Wrapper around a persistence point.  All input/output files in the application
 * (including intermediate steps from modules) must have a corresponding SmvFile instance.
 * This is true even if the intermedidate step does *NOT* need to be persisted.  The
 * nickname will be used to link modules together.
 */
case class SmvFile(_name: String, basePath: String, csvAttributes: CsvAttributes) extends
    SmvDataSet(s"Input file: ${_name}@${basePath}") {

  // TODO: name should be just the basePath? or basename of the basePath?
  override def name() = _name
  override def fullPath() = dataDir + "/" + basePath

  override def rdd(app: SmvApp): SchemaRDD = {
    implicit val ca = csvAttributes

    app.sqlContext.csvFileWithSchema(fullPath())
  }

  override def requiresDS() = Seq.empty
}


/**
 * Wrapper around the run parameters to aid map Modules to their names and vice-versa.
 */
class SmvRunParam(val rddMap: Map[String, SchemaRDD]) {
  def apply(rddName: String) = rddMap(rddName)
  def apply(ds: SmvDataSet) = rddMap(ds.name)
  def size() = rddMap.size
}

/**
 * base module class.  All SMV modules need to extend this class and provide their
 * description and dependency requirements (what does it depend on).
 * The module run method will be provided the result of all dependent inputs and the
 * result of the run is the result of the module.  All modules that depend on this module
 * will be provided the SchemaRDD result from the run method of this module.
 * Note: the module should *not* persist any RDD itself.
 */
abstract class SmvModule(_description: String) extends
  SmvDataSet(_description) {

  override val name = this.getClass().getName().filterNot(_=='$')

  type runParams = SmvRunParam
  def run(inputs: runParams) : SchemaRDD

  // TODO: this shouldn't be part of SmvModule.  Only app should know about persistance.
  // TODO: should probably convert "." in name to path separator "/"
  override def fullPath() = s"${dataDir}/output/${name}.csv"

  override def rdd(app: SmvApp): SchemaRDD = {
    run(new SmvRunParam(allRequireNames().map(r => (r, app.resolveRDD(r))).toMap))
  }
}

/**
 * Driver for SMV applications.  An application needs to override the getDataSets method
 * that provide the list of ALL known SmvFiles (in/out) and the list of modules to run.
 */
abstract class SmvApp (val appName: String, _sc: Option[SparkContext] = None) {

  val conf = new SparkConf().setAppName(appName)
  val sc = _sc.getOrElse(new SparkContext(conf))
  val sqlContext = new SQLContext(sc)
  private val mirror = ru.runtimeMirror(this.getClass.getClassLoader)

  /** stack of items currently being resolved.  Used for cyclic checks. */
  val resolveStack: mutable.Stack[String] = mutable.Stack()

  /** cache of all RDD that have already been resolved (executed) */
  val rddCache: mutable.Map[String, SchemaRDD] = mutable.Map()

  /** concrete applications need to provide list of datasets in application. */
  def getDataSets() : Seq[SmvDataSet] = Seq.empty

  /** concrete applications can provide a list of package names containing modules. */
  def getModulePackages() : Seq[String] = Seq.empty

  /** sequence of ALL known datasets.  Those specified by users and discovered in packages. */
  private def allDataSets(): Seq[SmvDataSet] = {
    getDataSets ++ getModulePackages.map(modulesInPackage).flatten
  }

  lazy private[smv] val allDataSetsByName = allDataSets.map(ds => (ds.name, ds)).toMap

  /** Get the RDD associated with given name. */
  def resolveRDD(name: String): SchemaRDD = {
    if (resolveStack.contains(name))
      throw new IllegalStateException(s"cycle found while resolving ${name}: " +
        resolveStack.mkString(","))

    resolveStack.push(name)

    val resRdd = rddCache.getOrElseUpdate(name, allDataSetsByName(name).rdd(this))

    val popRdd = resolveStack.pop()
    if (popRdd != name)
      throw new IllegalStateException(s"resolveStack corrupted.  Got ${popRdd}, expected ")

    resRdd
  }

  def resolveRDD(mod: SmvModule): SchemaRDD = resolveRDD(mod.name)

  /** maps the FQN of module name to the module object instance. */
  private[smv] def moduleNametoObject(modName: String) = {
    mirror.reflectModule(mirror.staticModule(modName)).instance.asInstanceOf[SmvModule]
  }

  def run(name: String) = {
    implicit val ca = CsvAttributes.defaultCsvWithHeader
    val modObject = moduleNametoObject(name)

    resolveRDD(modObject).saveAsCsvWithSchema(modObject.fullPath)
  }

  /** extract instances (objects) in given package that implement SmvModule. */
  private[smv] def modulesInPackage(pkgName: String): Seq[SmvModule] = {
    import com.google.common.reflect.ClassPath
    import scala.collection.JavaConversions._
    import scala.util.Try

    ClassPath.from(this.getClass.getClassLoader).
      getTopLevelClasses(pkgName).
      map(c => Try(moduleNametoObject(c.getName))).
      filter(_.isSuccess).
      map(_.get).
      toSeq
  }
}

/**
 * contains the module level dependency graph starting at the given startNode.
 * All prefixes given in packagePrefixes are removed from the output module/file name
 * to make the graph cleaner.  For example, project X at com.foo should probably add
 * a package prefix of "com.foo.X" to remove the repeated noise of "com.foo.X" before
 * every module name in the graph.
 */
class SmvModuleDependencyGraph(val startMod: SmvModule, val app: SmvApp,
                               val packagePrefixes: Seq[String] = Seq.empty) {
  def this(startNode: String, app: SmvApp, prefixes: Seq[String]) =
    this(app.moduleNametoObject(startNode), app, prefixes)

  type dependencyMap = Map[String, Set[String]]

  private def depsByName(nodeName: String) = app.allDataSetsByName(nodeName).allRequireNames()

  private def addDependencyEdges(nodeName: String, nodeDeps: Seq[String], map: dependencyMap): dependencyMap = {
    if (map.contains(nodeName)) {
      map
    } else {
      nodeDeps.foldLeft(map.updated(nodeName, nodeDeps.toSet))(
        (m,r) => addDependencyEdges(r, depsByName(r), m))
    }
  }

  private[smv] lazy val graph = {
    addDependencyEdges(startMod.name, startMod.allRequireNames(), Map())
  }

  private lazy val allFiles = graph.values.flatMap(vs =>
    vs.filter(v => app.allDataSetsByName(v).isInstanceOf[SmvFile]))

  private def stripPackagePrefix(nodeName: String) =
    packagePrefixes.map(_ + ".").foldLeft(nodeName)((s,p) => s.stripPrefix(p))

  /** quoted/clean name in graph output */
  private def q(s: String) = "\"" + stripPackagePrefix(s) + "\""

  private def fileStyles() = {
    allFiles.map(f => s"  ${q(f)} " + "[shape=box, color=\"pink\"]")
  }

  private def filesRank() = {
    Seq("{ rank = same; " + allFiles.map(f => s"${q(f)};").mkString(" ") + " }")
  }

  private def generateGraphvisCode() = {
    Seq(
      "digraph G {",
      "  rankdir=\"LR\";",
      "  node [style=filled,color=\"lightblue\"]") ++
      fileStyles() ++
      filesRank() ++
      graph.flatMap{case (k,vs) => vs.map(v => s"""  ${q(v)} -> ${q(k)} """ )} ++
      Seq("}")
  }

  def saveToFile(filePath: String) = {
    val pw = new PrintWriter(new File(filePath))
    generateGraphvisCode().foreach(pw.println)
    pw.close()
  }
}

