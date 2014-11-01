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

import org.apache.spark.sql.{SchemaRDD, SQLContext}
import org.apache.spark.{SparkContext, SparkConf}

import scala.collection.mutable

/**
 * Dependency management unit within the SMV application framework.  Execution order within
 * the SMV application framework is derived from dependency between SmvDataSet instances.
 * Instances of this class can either be a file or a module. In either case, there would
 * be a single result SchemaRDD.
 */
abstract class SmvDataSet(val name: String, val description: String) {
  private[smv] val dataDir = sys.env.getOrElse("DATA_DIR", "/DATA_DIR_ENV_NOT_SET")

  /** full path of file associated with this module/file */
  def fullPath(): String

  /** returns the SchemaRDD from this dataset (file/module) */
  def rdd(app: SmvApp): SchemaRDD
}

/**
 * Wrapper around a persistence point.  All input/output files in the application
 * (including intermediate steps from modules) must have a corresponding SmvFile instance.
 * This is true even if the intermedidate step does *NOT* need to be persisted.  The
 * nickname will be used to link modules together.
 */
case class SmvFile(_name: String, basePath: String, csvAttributes: CsvAttributes) extends
    SmvDataSet(_name, s"Input file: ${_name}@${basePath}") {

  override def fullPath() = dataDir + "/" + basePath

  override def rdd(app: SmvApp): SchemaRDD = {
    implicit val ca = csvAttributes

    app.sqlContext.csvFileWithSchema(fullPath())
  }
}

/**
 * base module class.  All SMV modules need to extend this class and provide their
 * name, description and dependency requirements (what does it depend on).
 * The module run method will be provided the result of all dependent inputs and the
 * result of the run is the result of the module.  All modules that depend on this module
 * will be provided the SchemaRDD result from the run method of this module.
 * Note: the module should *not* persist any RDD itself.
 */
abstract class SmvModule(_name: String, _description: String = "unknown") extends
  SmvDataSet(_name, _description) {

  def requires() : Seq[String]
  def run(inputs: Map[String, SchemaRDD]) : SchemaRDD

  // TODO: should probably convert "." in name to path separator "/"
  override def fullPath() = s"${dataDir}/output/${name}.csv"

  override def rdd(app: SmvApp): SchemaRDD = {
    run(requires().map(r => (r, app.resolveRDD(r))).toMap)
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

  lazy private val allDataSetsByName = allDataSets.map(ds => (ds.name, ds)).toMap

  /** Get the RDD associated with given name. */
  def resolveRDD(name: String) = {
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

  def run(name: String) = {
    implicit val ca = CsvAttributes.defaultCsvWithHeader

    resolveRDD(name).saveAsCsvWithSchema(allDataSetsByName(name).fullPath)
  }

  private def addDependencyEdges(nodeName: String, dependencyMap: mutable.Map[String, Set[String]]) {
    if (!dependencyMap.contains(nodeName)) {
      val ds = allDataSetsByName(nodeName)

      dependencyMap(nodeName) = if (ds.isInstanceOf[SmvModule]) {
        ds.asInstanceOf[SmvModule].requires().toSet
      } else {
        Set[String]()
      }

      dependencyMap(nodeName).foreach(d => addDependencyEdges(d, dependencyMap))
    }
  }

  def dependencyGraph(startNode: String) = {
    val sb = mutable.StringBuilder.newBuilder
    val edges : mutable.Map[String, Set[String]] = mutable.Map()
    addDependencyEdges(startNode, edges)

    Seq(
      "digraph G {",
      "  node [style=filled,color=\"lightblue\"]") ++
    edges.flatMap{case (k,vs) => vs.map(v => s"""  "${v}" -> "${k}" """ )} ++
    Seq("}")
  }

  /** extract instances (objects) in given package that implement SmvModule. */
  private[smv] def modulesInPackage(pkgName: String): Seq[SmvModule] = {
    import com.google.common.reflect.ClassPath
    import scala.collection.JavaConversions._
    import scala.reflect.runtime.{universe => ru}
    import scala.util.Try

    val cl = this.getClass.getClassLoader
    val m = ru.runtimeMirror(cl)

    ClassPath.from(cl).
      getTopLevelClasses(pkgName).
      map(c => Try(
        m.reflectModule(m.staticModule(c.getName)).instance.asInstanceOf[SmvModule])).
      filter(_.isSuccess).
      map(_.get).
      toSeq
  }
}

