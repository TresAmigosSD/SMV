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
import org.apache.spark.sql._
import org.apache.spark.sql.functions._

import org.joda.time._
import org.joda.time.format._

import graph._

/**
 * Provide functions for the interactive shell
 *
 * In SMV's `tools/conf/smv_shell_init.scala` or project's `conf/shell_init.scala` add
 * {{{
 * import org.tresamigos.smv.shell._
 * }}}
 **/
package object shell {
  private val appGU = new SmvGraphUtil(SmvApp.app.stages)

  def lsStage = SmvApp.app.stages.stageNames.foreach(println)

  /**
   * list all datasets in a stage
   * @param stageName could be the FQN or just the basename
   */
  def ls(stageName: String) = println(appGU.createDSList(SmvApp.app.stages.findStage(stageName)))

  /**
   * list all the datasets in the entire project
   **/
  def ls = println(appGU.createDSList())

  /**
   * list `dead` datasets in a stage
   * `dead` dataset is defined as "no contribution to the Output modules of the stage"
   * @param stageName could be the FQN or the basename
   **/
  def lsDead(stageName: String) = println(appGU.createDeadDSList(SmvApp.app.stages.findStage(stageName)))

  /**
   * list `dead` datasets in the entire project
   **/
  def lsDead = println(appGU.createDeadDSList())

  /**
   * list `leaf` datasets in a stage
   * `leaf` dataset is defined as "no modules in the stage depend on it, excluding Output modules"
   * Note: a `leaf` dataset must be `dead`, but some `dead` datasets are Not `leaf`s
   * @param stageName could be the FQN or the basename
   */
  def lsLeaf(stageName: String) = println(appGU.createLeafDSList(SmvApp.app.stages.findStage(stageName)))

  /**
   * list `leaf` datasets in the entire project
   **/
  def lsLeaf = println(appGU.createLeafDSList())

  /** take a stage name and print all DS in this stage, without unused input DS */
  def graph(stageName: String) = {
    val singleStgGU = new SmvGraphUtil(new SmvStages(Seq(SmvApp.app.stages.findStage(stageName))))
    println(singleStgGU.createDSAsciiGraph())
  }

  /** take no parameter, print stages and inter-stage links */
  def graph() = println(appGU.createStageAsciiGraph())

  /** take a DS, print in-stage dependency of that DS */
  def graph(ds: SmvDataSet) = println(appGU.createDSAsciiGraph(Seq(ds)))

  /**
   * list all `ancestors` of a dataset
   * `ancestors` are datasets current dataset depends on, directly or in-directly,
   * even include datasets from other stages
   **/
  def ancestors(ds: SmvDataSet) = println(appGU.createAncestorDSList(ds))

  /**
   * list all `descendants` of a dataset
   * `descendants` are datasets which depend on the current dataset directly or in-directly,
   * even include datasets from other stages
   **/
  def descendants(ds: SmvDataSet) = println(appGU.createDescendantDSList(ds))

  /**
   * Display a dataframe row in transposed view.
   */
  def peek(df: DataFrame, pos: Int = 1) = {
    df.peek(pos)
  }

  /**
   * Print current time
   **/
  def now() = {
    val fmt = DateTimeFormat.forPattern("HH:mm:ss")
    println(fmt.print(DateTime.now()))
  }
}
