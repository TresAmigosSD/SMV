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

private[smv] object FQN {
  /**
   * extract the basename of a given FQN.
   * For example: "a.b.c" --> "c"
   */
  def extractBaseName(fqn: String) : String = fqn.substring(fqn.lastIndexOf('.') + 1)

  /**
   * Remove package name from a given FQN.
   * e.g. "a.b.input.c" with package "a.b" --> "input.c"
   **/
  def removePrefix(fqn: String, pkg: String): String = {
    val prefix = pkg + "."
    fqn.startsWith(prefix) match {
      case true => fqn.substring(prefix.length, fqn.length)
      /** if the given prefix is not really a prefix, return the full string back */
      case false => fqn
      //case false => throw new IllegalArgumentException(s"${prefix} is not a prefix of ${fqn}")
    }
  }

  /** Find common prefix FQN from a Seq of FQNs */
  def sharedPrefix(fqns: Seq[String]): String = {
    if (fqns.isEmpty) ""
    else fqns.reduce{(l,r) =>
        (l.split('.') zip r.split('.')).
          collect{ case (a, b) if (a==b) => a}.mkString(".")
      }
  }
}
