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

/*
 * Universal Resource Name (URN) is the unique name for an SmvDataSet. There are
 * 2 types of URN: LinkURN and ModURN. Both are constructed from a fully qualified
 * name (FQN). LinkURN names an SmvDataSet which is a link to an SmvDataSet with
 * the given FQN. ModURN names an SmvDataSet which has with the given FQN and which
 * is not a link.
 */
sealed abstract class URN(prefix: String) {
  def fqn: String
  def getStage: Option[String] = {
    val allStageNames = SmvApp.app.smvConfig.stageNames

    allStageNames.find { stageName =>
      fqn.startsWith(stageName + ".")
    }
  }

  override def toString: String = s"${prefix}:${fqn}"
  def toModURN: ModURN          = ModURN(fqn)
  def toLinkURN: LinkURN        = LinkURN(fqn)
}

/*
 * Every concrete subclass of URN should be a case class to ensure
 * structural equality - this is important because we use the URN as a key
 */
case class LinkURN(fqn: String) extends URN("link") {
  override def toString: String = super.toString
}

case class ModURN(fqn: String) extends URN("mod") {
  override def toString: String = super.toString
}

/**
 * Factory which constructs the correct type of URN object given the URN as a string.
 */
object URN {
  private object errors {
    def invalidURN(urn: String) = new SmvRuntimeException(s"Invalid urn: ${urn}")
  }

  def apply(urn: String): URN = {
    val splitIdx = urn.lastIndexOf(':')
    if (splitIdx < 0)
      throw errors.invalidURN(urn)
    val fqn    = urn.substring(splitIdx + 1)
    val prefix = urn.substring(0, splitIdx)
    prefix match {
      case "mod"  => ModURN(fqn)
      case "link" => LinkURN(fqn)
      case _      => throw errors.invalidURN(urn)
    }
  }
}
