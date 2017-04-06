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

package org.tresamigos.smv.shell
import org.apache.spark.sql._
import org.apache.spark.sql.functions._

import org.joda.time._
import org.joda.time.format._

import org.tresamigos.smv._

/**
 * Provide functions for the interactive shell
 *
 * In SMV's `tools/conf/smv_shell_init.scala` or project's `conf/shell_init.scala` add
 * {{{
 * import org.tresamigos.smv.shell._
 * }}}
 **/
object ShellCmd {
  import org.tresamigos.smv.graph._

  private val appGU = new SmvGraphUtil(SmvApp.app)
  private val dsm   = SmvApp.app.dsm

  /** Resolve the ds, since user's input might not be resolved yet */
  private def load(ds: SmvDataSet): SmvDataSet = dsm.load(ds.urn).head

  /**
   * list all the smv-shell commands
   **/
  def help =
    """Here is a list of SMV-shell command
      |
      |Please refer to the API doc for details:
      |http://tresamigossd.github.io/SMV/scaladocs/index.html#org.tresamigos.smv.shell.package
      |
      |* lsStage
      |* ls
      |* ls(stageName: String)
      |* lsDead
      |* lsDead(stageName: String)
      |* lsDeadLeaf
      |* lsDeadLeaf(stageName: String)
      |* graph
      |* graph(ds: SmvDataSet)
      |* graph(stageName: String)
      |* ancestors(dsName: String)
      |* descendants(dsName: String)
      |* peek(df: DataFrame, pos: Int = 1)
      |* openCsv(path: String, ca: CsvAttributes = null, parserCheck: Boolean = false)
      |* openHive(tabelName: String)
      |* now
      |* df(ds: SmvDataSet)
      |* ddf(fqn: String)
      |* discoverSchema(path: String, n: Int = 100000, ca: CsvAttributes = CsvWithHeader)
      """.stripMargin

  /**
   * list all the stages
   **/
  def lsStage = SmvApp.app.stages.mkString("\n")

  /**
   * list all datasets in a stage
   * @param stageName could be the FQN or just the basename
   **/
  def ls(stageName: String) = appGU.createDSList(dsm.inferStageFullName(stageName))

  /**
   * list all the datasets in the entire project
   **/
  def ls = appGU.createDSList()

  /**
   * list `dead` datasets in a stage
   * `dead` dataset is defined as "no contribution to the Output modules of the stage"
   * @param stageName could be the FQN or the basename
   **/
  def lsDead(stageName: String) = appGU.createDeadDSList(dsm.inferStageFullName(stageName))

  /**
   * list `dead` datasets in the entire project
   **/
  def lsDead = appGU.createDeadDSList()

  /**
   * list `deadLeaf` datasets in a stage
   * `deadLeaf` dataset is defined as "no modules in the stage depend on it, excluding Output modules"
   * Note: a `deadLeaf` dataset must be `dead`, but some `dead` datasets are Not `leaf`s
   * @param stageName could be the FQN or the basename
   */
  def lsDeadLeaf(stageName: String) = appGU.createDeadLeafDSList(dsm.inferStageFullName(stageName))

  /**
   * list `leaf` datasets in the entire project
   **/
  def lsDeadLeaf = appGU.createDeadLeafDSList()

  /** take no parameter, print stages and inter-stage links */
  def _graphStage() = appGU.createStageAsciiGraph()

  /** take a stage name and print all DS in this stage, without unused input DS */
  def _graph(stageName: String) = {
    val singleStgGU = new SmvGraphUtil(SmvApp.app, Seq(dsm.inferStageFullName(stageName)))
    singleStgGU.createDSAsciiGraph()
  }

  def _graph() = appGU.createDSAsciiGraph()

  /** take a DS, print in-stage dependency of that DS */
  def _graph(ds: SmvDataSet) = appGU.createDSAsciiGraph(Seq(load(ds)))

  /**
   * list all `ancestors` of a dataset
   * `ancestors` are datasets current dataset depends on, directly or in-directly,
   * even include datasets from other stages
   **/
  def ancestors(ds: SmvDataSet) = appGU.createAncestorDSList(load(ds))
  def ancestors(dsName: String) = appGU.createAncestorDSList(dsm.inferDS(dsName).head)

  /**
   * list all `descendants` of a dataset
   * `descendants` are datasets which depend on the current dataset directly or in-directly,
   * even include datasets from other stages
   **/
  def descendants(ds: SmvDataSet) = appGU.createDescendantDSList(ds)
  def descendants(dsName: String) = appGU.createDescendantDSList(dsm.inferDS(dsName).head)

  /**
   * Print current time
   **/
  def now() = {
    val fmt = DateTimeFormat.forPattern("HH:mm:ss")
    fmt.print(DateTime.now())
  }

  /**
   * Read in a Hive table as DF
  **/
  def openHive(tableName: String) = {
    new SmvHiveTable(tableName).rdd()
  }

  /**
   * Read in a Csv file as DF
  **/
  def openCsv(path: String, ca: CsvAttributes, parserCheck: Boolean): DataFrame = {

    /** isFullPath = true to avoid prepending data_dir */
    object file extends SmvCsvFile(path, ca, null, true) {
      override val forceParserCheck   = false
      override val failAtParsingError = parserCheck
    }
    file.rdd()
  }

  def openCsv(path: String): DataFrame = openCsv(path, null, false)

  /**
   * Resolve SmvDataSet
   *
   * @param ds an SmvDataSet
   * @return result DataFrame
  **/
  def df(ds: SmvDataSet) = {
    hotdeployIfCapable(ds, getClass.getClassLoader)
    SmvApp.app.runModule(ds.urn)
  }

  /**
   * Reload modules using custom class loader
   **/
  private[smv] def hotdeployIfCapable(ds: SmvDataSet,
                                      cl: ClassLoader = getClass.getClassLoader): Unit = {
    import scala.reflect.runtime.universe

    val mir  = universe.runtimeMirror(cl).reflect(SmvApp.app.sc)
    val meth = mir.symbol.typeSignature.member(universe.newTermName("hotdeploy"))

    if (meth.isMethod) {
      mir.reflectMethod(meth.asMethod)()
    } else {
      println("hotdeploy is not available in the current SparkContext")
    }
  }
}
