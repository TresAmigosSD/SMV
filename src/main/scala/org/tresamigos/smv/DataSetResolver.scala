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

import org.joda.time.DateTime

/**
 * DataSetResolver (DSR) is the entrypoint through which the DataSetMgr acquires
 * SmvDataSets. A DSR object represent a single transaction. Each DSR creates a
 * set of DataSetRepos at instantiation. When asked for an SmvDataSet, DSR queries
 * the repos for that SmvDataSet and resolves it. The SmvDataSet is responsible for
 * resolving itself, given access to the DSR to load/resolve the SmvDataSet's
 * dependencies. DSR caches the SmvDataSets it has already resolved to ensure that
 * any SmvDataSet is only resolved once.
 */
class DataSetResolver(val repos: Seq[DataSetRepo],
                      smvConfig: SmvConfig) {
  /**
   * Timestamp which will be injected into the resolved SmvDataSets
   */
  val transactionTime = new DateTime

  // URN to resolved SmvDataSet
  var urn2res: Map[URN, SmvDataSet] = Map.empty

  /**
   * Given URN, return cached resolved version SmvDataSet if it exists, or
   * otherwise load unresolved version from source and resolve it.
   */
  def loadDataSet(urns: URN*): Seq[SmvDataSet] =
    urns map { urn =>

      val found = urn.getStage.isDefined
      if (!found) {
        throw new SmvRuntimeException(s"""Cannot find module with FQN [${urn.fqn}]. Is the stage name specified in the config?""")
      }

      urn2res.get(urn).getOrElse {
        val ds = urn match {
          case lUrn: LinkURN =>
            throw new SmvRuntimeException(s"""LinkURN ${lUrn} is not supported any more""")
          case mUrn: ModURN =>
            findDataSetInRepo(mUrn)
        }
        resolveDataSet(ds)
      }
    }

  /*
   * Track which SmvDataSets is currently being resolved. Used to check for
   * dependency cycles. Note: we no longer have to worry about corruption of
   * resolve stack because a new stack is created per transaction.
   */
  var resolveStack: Seq[URN] = Seq.empty

  /**
   * Return cached resolved version of given SmvDataSet if it exists, or resolve
   * it otherwise.
   */
  def resolveDataSet(ds: SmvDataSet): SmvDataSet = {
    if (resolveStack.contains(ds.urn))
      throw new IllegalStateException(msg.dependencyCycle(ds, resolveStack))
    else
      urn2res.get(ds.urn).getOrElse {
        resolveStack = ds.urn +: resolveStack
        val resolvedDs = ds.resolve(this)
        resolvedDs.setTimestamp(transactionTime)
        urn2res = urn2res + (ds.urn -> resolvedDs)
        resolveStack = resolveStack.tail
        resolvedDs
      }
  }

  /**
   * Given a URN, findDataSetInRepo recursively searches for an unresolved SmvDataSet
   * in each repo via a head/tail algorithm. Checking if a repo has an SmvDataSet
   * before loading it would incur the same cost twice, so we simply Try loading
   * the SmvDataSet from each repo and move on to the next repo if it fails.
   */
  private def findDataSetInRepo(urn: ModURN, reposToTry: Seq[DataSetRepo] = repos): SmvDataSet = {
    if(reposToTry.isEmpty)
      throw new SmvRuntimeException(msg.dsNotFound(urn))
    else
      reposToTry.head.loadDataSet(urn) match {
        case None     => findDataSetInRepo(urn, reposToTry.tail)
        case Some(ds) => ds
      }
  }

  /**
   * msg encapsulates the messages which will be thrown as errors or printed as
   * warnings for the user.
   */
  object msg {
    def dsNotFound(urn: URN): String = s"SmvDataSet ${urn} not found"
    def dependencyCycle(ds: SmvDataSet, s: Seq[URN]): String =
      s"Cycle found while resolving ${ds.urn}: " + s.foldLeft("")((acc, urn) => s"${acc},${urn}")
  }
}
