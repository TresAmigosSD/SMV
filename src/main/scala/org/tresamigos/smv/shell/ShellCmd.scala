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
package shell

import org.apache.spark.sql.{DataFrame}

import org.joda.time.DateTime
import org.joda.time.format.DateTimeFormat

/**
 * Provide functions for the interactive shell
 *
 * In SMV's `tools/conf/smv_shell_init.scala` or project's `conf/shell_init.scala` add
 * {{{
 * import org.tresamigos.smv.shell._
 * }}}
 **/
object ShellCmd {
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
      |http://tresamigossd.github.io/SMV/scaladocs/1.5.2.8/index.html#org.tresamigos.smv.shell.package
      |
      |* lsStage
      |* ls
      |* ls(stageName: String)
      |* lsDead
      |* lsDead(stageName: String)
      |* lsDeadLeaf
      |* lsDeadLeaf(stageName: String)
      |* ancestors(dsName: String)
      |* descendants(dsName: String)
      |* peek(df: DataFrame, pos: Int = 1)
      |* openCsv(path: String, ca: CsvAttributes = null, parserCheck: Boolean = false)
      |* openHive(tabelName: String)
      |* exportToHive(dsName: String)
      |* now
      |* df(ds: SmvDataSet)
      |* ddf(fqn: String)
      |* smvDiscoverSchemaToFile(path: String, n: Int = 100000, ca: CsvAttributes = CsvWithHeader)
      """.stripMargin

  def _edd(name: String, collector: SmvRunInfoCollector=new SmvRunInfoCollector): String =
    dsm.inferDS(name).head.getEdd(collector=collector)

  def edd(name: String): Unit =
    println(_edd(name))
}