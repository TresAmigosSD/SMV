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
import scala.collection.mutable


class DataSetResolver(repoFactories: Seq[DataSetRepoFactory]) {
  val repos = repoFactories.map( _.createRepo )
  val resolved: mutable.Map[URN, SmvDataSet] = mutable.Map.empty

  object errors {
    def dsNotFound(fqn: String): SmvRuntimeException =
      new SmvRuntimeException(s"SmvDataSet ${fqn} not found")
    def dependencyCycle(ds: SmvDataSet, s: mutable.Stack[SmvDataSet]): IllegalStateException =
      new IllegalStateException(s"cycle found while resolving ${ds.urn}: " + s.foldLeft("")((acc, ds) => s"${acc},${ds.urn}"))
  }

  def loadDataSet(urns: URN*): Seq[SmvDataSet] = {
    urns map {
      urn =>
        Try(resolved(urn)) match {
          case Success(ds) => ds
          case Failure(_) =>
            val ds = urn match {
              case LinkURN(_) => new SmvModuleLink(findModInRepoList(urn.fqn, repos).asInstanceOf[SmvOutput])
              case ModURN(_) => findModInRepoList(urn.fqn, repos)
            }
            val resolvedDs: SmvDataSet = Try(resolveDataSet(ds)) match {
              case Success(ds) => ds
              case Failure(e) =>
                println(e.getMessage)
                throw e
            }
            resolvedDs
        }
    }
  }

  val resolveStack: mutable.Stack[SmvDataSet] = mutable.Stack()

  def resolveDataSet(ds: SmvDataSet): SmvDataSet = {
    if (resolveStack.contains(ds))
      throw errors.dependencyCycle(ds, resolveStack)
    else {
      val urn = URN(ds.urn)
      Try(resolved(urn)) match {
        case Success(resolvedDs) => resolvedDs
        case Failure(_) =>
          resolveStack.push(ds)
          val resolvedDs = ds.resolve(this)
          resolved += (urn -> resolvedDs)
          resolveStack.pop()
          resolvedDs
      }
    }
  }

  // Recursively search for ds in repos. Throw error if not found
  private def findModInRepoList(fqn: String, repoList: Seq[DataSetRepo]): SmvDataSet = {
    Try( repoList.head ) match {
      // If repoList is empty, dataset not found
      case Failure(_) =>
        val e = errors.dsNotFound(fqn)
        println(e.getMessage)
        throw e
      case Success(repo) =>
        Try( repo.loadDataSet(fqn) ) match {
          // If dataset not found in repo, try next repo
          case Failure(_) => findModInRepoList( fqn, repoList.filterNot(repoInList => repo == repoInList) )
          case Success(ds) => ds
        }
    }
  }
}
