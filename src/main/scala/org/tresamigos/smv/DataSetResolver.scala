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

import scala.annotation.tailrec

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
            val dsFound = loadDataSet(lUrn.toModURN).head
            new SmvModuleLink(dsFound.asInstanceOf[SmvOutput])
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
  var resolveStack: Seq[SmvDataSet] = Seq.empty

  /**
   * Return cached resolved version of given SmvDataSet if it exists, or resolve
   * it otherwise.
   */
  def resolveDataSet(ds: SmvDataSet): SmvDataSet = {
    if (resolveStack.contains(ds))
      throw new IllegalStateException(msg.dependencyCycle(ds, resolveStack))
    else
      urn2res.get(ds.urn).getOrElse {
        resolveStack = ds +: resolveStack
        val resolvedDs = ds.resolve(this)
        resolvedDs.setTimestamp(transactionTime)
        urn2res = urn2res + (ds.urn -> resolvedDs)
        resolveStack = resolveStack.tail
        validateDependencies(resolvedDs)
        resolvedDs
      }
  }

  /**
   * Check dependency rules and report all violations. If there are violations
   * and SMV isn't configured to ignore dependency violations, throw exception.
   */
  def validateDependencies(ds: SmvDataSet): Unit = {
    if (!ds.isInstanceOf[SmvModuleLink]) {
      val dsStage = ds.parentStage.get
      ds.resolvedRequiresDS foreach {dep =>
        dep match {
          case l: SmvModuleLink =>
            if (dsStage == l.smvModule.parentStage.get)
              throw new SmvRuntimeException(msg.sameStageLink(ds.urn, dsStage))
          case _ =>
            val depStage = dep.parentStage.get
            if (dsStage != depStage)
              throw new SmvRuntimeException(msg.crossStageDependency(ds.urn, dsStage, dep.urn, depStage))
        }
      }
    }
  }

  /**
   * Given a URN, findDataSetInRepo recursively searches for an unresolved SmvDataSet
   * in each repo via a head/tail algorithm. Checking if a repo has an SmvDataSet
   * before loading it would incur the same cost twice, so we simply Try loading
   * the SmvDataSet from each repo and move on to the next repo if it fails.
   */
  @tailrec
  private def findDataSetInRepo(urn: ModURN, reposToTry: Seq[DataSetRepo] = repos): SmvDataSet =
    reposToTry match {
      case head :: rest =>
        head.loadDataSet(urn) match {
          case Some(ds) => ds
          case _        => findDataSetInRepo(urn, rest)
        }
      case _ =>
        throw new SmvRuntimeException(msg.dsNotFound(urn))
    }

  /**
   * msg encapsulates the messages which will be thrown as errors or printed as
   * warnings for the user.
   */
  object msg {
    def dsNotFound(urn: URN): String = s"SmvDataSet ${urn} not found"
    def crossStageDependency(dsUrn: URN, dsStage: String, depUrn: URN, depStage: String): String =
      s"Module ${dsUrn} in ${dsStage} must use SmvModuleLink to depend on module ${depUrn} in ${depStage}"
    def sameStageLink(linkUrn: URN, stage: String): String =
      s"SmvModuleLink ${linkUrn} cannot link to ${linkUrn.toModURN} because they belong to the same stage"
    def dependencyCycle(ds: SmvDataSet, s: Seq[SmvDataSet]): String =
      s"Cycle found while resolving ${ds.urn}: " + s.foldLeft("")((acc, ds) => s"${acc},${ds.urn}")
  }
}
