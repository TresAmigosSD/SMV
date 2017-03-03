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


class DataSetResolver(repoFactories: Seq[DataSetRepoFactory], smvConfig: SmvConfig, depRules: Seq[DependencyRule]) {
  val repos = repoFactories.map( _.createRepo )
  val resolved: mutable.Map[URN, SmvDataSet] = mutable.Map.empty

  def loadDataSet(urns: URN*): Seq[SmvDataSet] =
    urns map {
      urn =>
        Try( resolved(urn) ) match {
          case Success(ds) => ds
          case Failure(_) =>
            val ds = urn match {
              case lUrn: LinkURN =>
                val dsFound = findDataSetInRepo(lUrn.toModURN)
                new SmvModuleLink(dsFound.asInstanceOf[SmvOutput])
              case mUrn: ModURN =>
                findDataSetInRepo(mUrn)
            }
            resolveDataSet(ds)
        }
    }

  // Note: we no longer have to worry about corruption of resolve stack
  // because a new stack is created per transaction
  val resolveStack: mutable.Stack[SmvDataSet] = mutable.Stack()

  def resolveDataSet(ds: SmvDataSet): SmvDataSet = {
    if (resolveStack.contains(ds))
      throw new IllegalStateException(msg.dependencyCycle(ds, resolveStack))
    else
      Try(resolved(ds.urn)) match {
        case Success(resolvedDs) => resolvedDs
        case Failure(_) =>
          resolveStack.push(ds)
          val resolvedDs = ds.resolve(this)
          resolved += (ds.urn -> resolvedDs)
          resolveStack.pop()
          validateDependencies(resolvedDs)
          resolvedDs
      }
  }

  def validateDependencies(ds: SmvDataSet): Unit = {
    val depViolations = depRules flatMap (_.check(ds))
    if(depViolations.size > 0) {
      println(msg.listDepViolations(ds, depViolations))
      if(smvConfig.permitDependencyViolation)
        println(msg.nonfatalDepViolation)
      else
        throw new IllegalStateException(msg.fatalDepViolation)
    }
  }

  private def findDataSetInRepo(urn: ModURN, reposToTry: Seq[DataSetRepo] = repos): SmvDataSet =
    Try(reposToTry.head) match {
      case Failure(_) => throw new SmvRuntimeException(msg.dsNotFound(urn))
      case Success(repo) =>
        Try(repo.loadDataSet(urn)) match {
          case Failure(_) => findDataSetInRepo(urn, reposToTry.tail)
          case Success(ds) => ds
        }
    }

  object msg {
    def dsNotFound(urn: URN): String = s"SmvDataSet ${urn} not found"
    def nonfatalDepViolation: String =
      "Continuing module resolution as the app is configured to permit dependency rule violation"
    def fatalDepViolation: String =
        s"Terminating module resolution when dependency rules are violated. To change this behavior, please run the app with option --${smvConfig.cmdLine.permitDependencyViolation.name}"
    def dependencyCycle(ds: SmvDataSet, s: mutable.Stack[SmvDataSet]): String =
      s"cycle found while resolving ${ds.urn}: " + s.foldLeft("")((acc, ds) => s"${acc},${ds.urn}")
    def listDepViolations(ds: SmvDataSet, vis: Seq[DependencyViolation]) =
      s"Module ${ds.urn} violates dependency rules" + mkViolationString(vis)
    private def mkViolationString(violations: Seq[DependencyViolation]): String =
      (for {
        v <- violations
        header = s".. ${v.description}"
      } yield
        (header +: v.components.map(m => s".... ${m.urn}")).mkString("\n")
      ).mkString("\n")
  }
}
