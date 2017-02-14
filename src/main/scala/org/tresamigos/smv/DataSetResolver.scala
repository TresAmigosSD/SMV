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

class DataSetResolver(repoFactories: Seq[DataSetRepoFactory]) {
  val repos = repoFactories.map( _.createRepo )

  object errors {
    def dsNotFound(fqn: String): SmvRuntimeException =
      new SmvRuntimeException(s"SmvDataSet ${fqn} not found")
  }

  def loadDataSet(fqn: String): SmvDataSet = {
    findModInRepoList(fqn, repos)
  }

  // Recursively search for ds in repos. Throw error if not found
  private def findModInRepoList(fqn: String, repoList: Seq[DataSetRepo]): SmvDataSet = {
    Try( repoList.head ) match {
      // If repoList is empty, dataset not found
      case Failure(_) => throw errors.dsNotFound(fqn)
      case Success(repo) =>
        Try( repo.loadDataSet(fqn) ) match {
          // If dataset not found in repo, try next repo
          case Failure(_) => findModInRepoList( fqn, repoList.filterNot(repoInList => repo == repoInList) )
          case Success(ds) => ds
        }
    }
  }
}
