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
package git

import java.io.File
import org.eclipse.jgit.lib.{Repository, RepositoryBuilder}
import org.eclipse.jgit.api.Git
import scala.collection.JavaConversions._

/**
 * Represents a local source control, backed by git, for user module code.
 *
 * @param workDir The working directory where source code and the `.git` directory exists
 */
case class SmvGit(workDir: String = ".") {
  import SmvGit._

  withRepo(workDir) { repo =>
    require(repo.getDirectory.exists, s"${workDir} does not have a git directory")
  }

  /** Adds a new file or changes to an existing file to the source control */
  def addFile(author: String, authorEmail: String, filePath: String, commitMessage: String): Unit =
    withRepo(workDir) { repo =>
      val git = new Git(repo)
      git.add.addFilepattern(filePath).call()
      git.commit.setCommitter(Committer, CommitterEmail)
        .setAuthor(author, authorEmail)
        .setMessage(commitMessage)
        .call()
  }

}

object SmvGit {
  val Committer: String = "SMV"
  val CommitterEmail: String = "smv@smv.org"

  def withRepo[T](workDir: String)(code: Repository => T) = {
    val repo = new RepositoryBuilder().readEnvironment().setWorkTree(new File(workDir)).build()
    try { code(repo) } finally { repo.close() }
  }
}
