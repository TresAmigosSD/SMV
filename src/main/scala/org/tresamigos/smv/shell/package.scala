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
import org.apache.spark.sql.Column
import org.apache.spark.sql.functions._

/**
 * Provide functions for the interactive shell
 *
 * In SMV's `tools/conf/smv_shell_init.scala` or project's `conf/shell_init.scala` add
 * {{{
 * import org.tresamigos.smv.shell._
 * }}}
 **/
package object shell {
  def lsStage = SmvApp.app.stages.stageNames.foreach(println)

  /**
   * list all datasets in a stage
   * @param stageName could be the FQN or just the basename
   */
  def ls(stageName: String) = println(new ListDataSets(SmvApp.app.stages.findStage(stageName)).list)

  /**
   * list all the datasets in the entire project
   **/
  def ls = println(new ListDataSets(SmvApp.app.stages).list)

  /**
   * list `dead` datasets in a stage
   * `dead` dataset is defined as "no contribution to the Output modules of the stage"
   * @param stageName could be the FQN or the basename
   **/
  def lsDead(stageName: String) = println(new ListDataSets(SmvApp.app.stages.findStage(stageName)).listDead)

  /**
   * list `dead` datasets in the entire project
   **/
  def lsDead = println(new ListDataSets(SmvApp.app.stages).listDead)

  /**
   * list `leaf` datasets in a stage
   * `leaf` dataset is defined as "no modules in the stage depend on it, excluding Output modules"
   * Note: a `leaf` dataset must be `dead`, but some `dead` datasets are Not `leaf`s
   * @param stageName could be the FQN or the basename
   */
  def lsLeaf(stageName: String) = println(new ListDataSets(SmvApp.app.stages.findStage(stageName)).listLeaf)

  /**
   * list `leaf` datasets in the entire project
   **/
  def lsLeaf = println(new ListDataSets(SmvApp.app.stages).listLeaf)

  /** take a stage name and print all DS in this stage, without unused input DS */
  def graph(stageName: String) = new DataSetAsciiGraph(SmvApp.app.stages.findStage(stageName)).show

  /** take no parameter, print stages and inter-stage links */
  def graph() = new StageAsciiGraph(SmvApp.app.stages).show

  /** take a DS, print in-stage dependency of that DS */
  def graph(ds: SmvDataSet) = new DataSetAsciiGraph(SmvApp.app.stages.findStageForDataSet(ds), Seq(ds)).show

  /**
   * list all `ancestors` of a dataset
   * `ancestors` are datasets current dataset depends on, directly or in-directly,
   * even include datasets from other stages
   **/
  def ancestors(ds: SmvDataSet) = println(new ListDataSets(SmvApp.app.stages).ancestors(ds))

  /**
   * list all `descendants` of a dataset
   * `descendants` are datasets which depend on the current dataset directly or in-directly,
   * even include datasets from other stages
   **/
  def descendants(ds: SmvDataSet) = println(new ListDataSets(SmvApp.app.stages).descendants(ds))
}
