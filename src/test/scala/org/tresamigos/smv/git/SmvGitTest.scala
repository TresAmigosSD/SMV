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

import sys.process._
import java.io.File

trait SmvGitTestFixture extends SmvUnitSpec {
  val TestDir = s"${TmpDir}/git"
  val GitDir = s"""${TestDir}/.git"""
}

class SmvGitTest extends SmvGitTestFixture {
  "SmvGit" should "fail if the current directory is not part of a git repository" in {
    s"rm -rf ${GitDir}".!!

    intercept[IllegalArgumentException] { SmvGit(GitDir) }
  }

}

class SmvGitAddTest extends SmvGitTestFixture {
  override def beforeEach() = {
    super.beforeEach()
    SmvGit.createRepo(GitDir)
  }

  override def afterEach() = {
    s"rm -rf ${TestDir}".!!
  }

  "SmvGit.addFile" should "be able to add a new file to the git repository" in {
    ( "echo hi" #> new File(s"${TmpDir}/git/f1") ).!!

    SmvGit(GitDir).addFile("Author X", "author.x@example.com", "f1", "add file f1")
    // TODO: check that file is committed
  }
}
