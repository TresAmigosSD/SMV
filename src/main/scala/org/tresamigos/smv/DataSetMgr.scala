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

import scala.util.{Try, Success, Failure}

class DataSetMgr {
  private var dsRepoFactories: Seq[DataSetRepoFactory] = Seq.empty[DataSetRepoFactory]

  def register(newRepoFactory: DataSetRepoFactory): Unit = {
    dsRepoFactories = dsRepoFactories :+ newRepoFactory
  }

  def load(urns: URN*): Seq[SmvDataSet] = {
    val resolver = new DataSetResolver(dsRepoFactories)
    resolver.loadDataSet(urns:_*)
  }

  def hasDataSet(urn: URN): Boolean =
    dsRepoFactories exists (_.createRepo.hasDataSet(urn.fqn))

  def allOutputModules(): Seq[URN] =
    dsRepoFactories flatMap (_.createRepo.allOutputModules) map (URN(_))

  def outputModsForStage(stageName: String): Seq[URN] =
    dsRepoFactories flatMap (_.createRepo.outputModsForStage(stageName)) map (URN(_))
}
