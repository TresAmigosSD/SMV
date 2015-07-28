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
import scala.util.Try

/**
 * Driver for SMV applications.  Most apps do not need to override this class and should just be
 * launched using the SmvApp object (defined below)
 */
class SmvApp (private val cmdLineArgs: Seq[String], _sc: Option[SparkContext] = None) {

  val smvConfig = new SmvConfig(cmdLineArgs)
  val isDevMode = smvConfig.cmdLine.devMode()
  val stages = smvConfig.stages
  val sparkConf = new SparkConf().setAppName(smvConfig.appName)
  val sc = _sc.getOrElse(new SparkContext(sparkConf))
  val sqlContext = new SQLContext(sc)
  private val mirror = ru.runtimeMirror(this.getClass.getClassLoader)

  /** stack of items currently being resolved.  Used for cyclic checks. */
  val resolveStack: mutable.Stack[String] = mutable.Stack()

  /** get list of all packages in this app (union of all stage packages) **/
  def getAllPackages() : Seq[String] = {
    stages.flatMap(s => s.pkgs)
  }

  /** concrete applications can provide a more interesting RejectLogger. 
   *  Example: override val rejectLogger = new SCRejectLogger(sc, 3)
   */
  def rejectLogger() : RejectLogger = TerminateRejectLogger
  
  def saveRejects(path: String) = {
    // TODO: isInstanceOf is evil.  Use a property of the logger instance instead!!!
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

  def allModules() = getAllPackages.map(modulesInPackage).flatten
  lazy val packagesPrefix = {
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
    val pathName = s"${smvConfig.appName}.json"
    new SmvModuleJSON(this, packages).saveToFile(pathName)
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
   * The main entry point into the app.  This will parse the command line arguments
   * to determine which modules should be run/graphed/etc.
   */
  def run() = {
    setSparkSqlConfigParams()

    if (smvConfig.cmdLine.json()) {
      genJSON()
    }

    smvConfig.cmdLine.modules().foreach { module =>
      val modObject = moduleNametoObject(module)

      if (smvConfig.cmdLine.graph()) {
        // TODO: need to combine the modules for graphs into a single graph.
        genDotGraph(modObject)
      } else {
        val modResult = resolveRDD(modObject)

        if (! isDevMode)
          modObject.persist(this, modResult)

        // create edd dump of the module in this directory
        smvConfig.cmdLine.eddDir.get map { dir =>
          val simpleName = module.split('.').last
          modResult.edd.addBaseTasks().saveReport(s"${dir}/${simpleName}")
        }
      }
    }
  }
}

/**
 * Common entry point for all SMV applications.  This is the object that should be provided to spark-submit.
 */
object SmvApp {
  def main(args: Array[String]) {
    new SmvApp(args).run()
  }
}


// TODO: json is a representation.  Need to rename this class to indicate WHAT it is actually generating not just the type.
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

