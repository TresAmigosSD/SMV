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
import org.eclipse.jgit.api.Git
import org.eclipse.jgit.treewalk.TreeWalk

import scala.sys.process._
import scala.collection.JavaConversions._

import SmvGit._

trait SmvGitTestFixture extends SmvUnitSpec {
  val TestDir = s"${TmpDir}/git"
}

class SmvGitTest extends SmvGitTestFixture {
  "SmvGit" should "fail if the current directory is not part of a git repository" in {
    s"rm -rf ${TestDir}/.git".!!

    intercept[IllegalArgumentException] { SmvGit(TestDir) }
  }

}

class SmvGitAddTest extends SmvGitTestFixture {
  override def beforeEach() = {
    super.beforeEach()
    withRepo(TestDir) { _.create() }
  }

  override def afterEach() = {
    s"rm -rf ${TestDir}".!!
  }

  def createFile(filepath: String, content: String): Unit =
    ( s"echo ${content}" #> new File(s"${TmpDir}/git/${filepath}") ).!!

  def assertGitFileContent(filepath: String, content: String): Unit = {
    var paths = Seq.empty[String]
    val buf = new java.io.ByteArrayOutputStream

    withRepo(TestDir) { repo =>
      val git = new Git(repo)
      val lastCommitTree = git.log.setMaxCount(1).call().toSeq.apply(0).getTree
      val walk = new TreeWalk(repo)
      walk.reset(lastCommitTree)
      while (walk.next()) {
        paths = walk.getPathString +: paths
        repo.open(walk.getObjectId(0)).copyTo(buf)
      }
      buf.close()
    }

    paths shouldBe Seq(filepath)
    buf.toString shouldBe s"${content}\n"
  }

  "SmvGit.addFile" should "be able to add a new file to the git repository" in {
    val path = "f1"
    val content = "hi"
    createFile(path, content)

    SmvGit(TestDir).addFile("Author X", "author.x@example.com", path, "add file f1")

    assertGitFileContent(path, content)
  }

  it should "be able to update an existing file content in the git repository" in {
    val path = "f2"
    val List(c1, c2) = List("first", "second")
    createFile(path, c1)
    SmvGit(TestDir).addFile("Author X", "author.x@example.com", path, "first commit")
    createFile(path, c2)
    SmvGit(TestDir).addFile("Author X", "author.x@example.com", path, "second commit")

    assertGitFileContent(path, c2)
  }
}
