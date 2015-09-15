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
import java.security.InvalidKeyException

import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.{SparkContext, SparkConf}

import scala.collection.mutable

/**
 * Driver for SMV applications.  Most apps do not need to override this class and should just be
 * launched using the SmvApp object (defined below)
 */
class SmvApp (private val cmdLineArgs: Seq[String], _sc: Option[SparkContext] = None) {

  val smvConfig = new SmvConfig(cmdLineArgs)
  val genEdd = smvConfig.cmdLine.genEdd()
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
  val sqlContext = new SQLContext(sc)

  // configure spark sql params and inject app here rather in run method so that it would be done even if we use the shell.
  setSparkSqlConfigParams()

  injectAppIntoAllDatasets()

  /** stack of items currently being resolved.  Used for cyclic checks. */
  val resolveStack: mutable.Stack[String] = mutable.Stack()

  /**
   * Create a DataFrame from string for temporary use (in test or shell)
   **/
  def createDF(schemaStr: String, data: String) = {
    val smvCF = SmvCsvData(schemaStr, data)
    smvCF.injectApp(this)
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
    SmvHDFS.purgeDirectory(smvConfig.outputDir, validFilesInOutputDir())
  }

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

    val resRdd = ds.rdd()

    val popRdd = resolveStack.pop()
    if (popRdd != dsName)
      throw new IllegalStateException(s"resolveStack corrupted.  Got ${popRdd}, expected ${dsName}")

    resRdd
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
   * inject the current app into all known data sets.
   */
  private def injectAppIntoAllDatasets() = {
    smvConfig.stages.injectApp(this)
  }

  /**
   * delete the current output files of the modules to run (and not all the intermediate modules).
   */
  private def deleteOutputModules() = {
    // TODO: replace with df.write.mode(Overwrite) once we move to spark 1.4
    smvConfig.modulesToRun().foreach {m => m.deleteOutputs()}
  }

  /**
   * The main entry point into the app.  This will parse the command line arguments
   * to determine which modules should be run/graphed/etc.
   */
  def run() = {
    if (smvConfig.cmdLine.json()) {
      genJSON()
    }

    println("Modules to run")
    println("--------------")
    smvConfig.modulesToRun().foreach(m => println(m.name))
    println("--------------")

    deleteOutputModules()

    if (smvConfig.cmdLine.purgeOldOutput()) {
      purgeOldOutputFiles()
    }

    smvConfig.modulesToRun().foreach { module =>

      if (smvConfig.cmdLine.graph()) {
        // TODO: need to combine the modules for graphs into a single graph.
        genDotGraph(module)
      } else {
        val modResult = resolveRDD(module)

        // if module was ephemeral, then it was not saved during graph execution and we need
        // to persist it here explicitly.
        if (module.isEphemeral)
          module.persist(modResult)
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
// TODO: this should be moved into stages (and accept a list of stages rather than packages)
private[smv] class SmvModuleJSON(app: SmvApp, packages: Seq[String]) {
  private def allModules = {
    if (packages.isEmpty) app.allAppModules
    else packages.map{app.packagesPrefix + _}.flatMap{ p => SmvReflection.objectsInPackage[SmvModule](p) }
  }.sortWith{(a,b) => a.name < b.name}

  private def allFiles = allModules.flatMap(m => m.requiresDS().filter(v => v.isInstanceOf[SmvFile]))

  private def printName(m: SmvDataSet) = m.name.stripPrefix(app.packagesPrefix)

  def generateJSON() = {
    "{\n" +
    allModules.map{m =>
      s"""  "${printName(m)}": {""" + "\n" +
      s"""    "version": ${m.version},""" + "\n" +
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
