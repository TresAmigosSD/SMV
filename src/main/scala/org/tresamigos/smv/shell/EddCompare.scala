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

package org.tresamigos.smv.shell

import org.tresamigos.smv._, edd._
import org.apache.spark.annotation._

/**
 * Provides list functions for a SmvStage or SmvStages
 **/
 @Experimental
object EddCompare {
  private def readEddFile(path: String) = SmvApp.app.sqlContext.read.json(path)
  def compareFiles(f1: String, f2: String) = EddResultFunctions(readEddFile(f1)).compareWith(readEddFile(f2))

  def compareDirs(d1: String, d2: String) = {
    val dir1Files = SmvHDFS.dirList(d1).filter{f => f.endsWith(".edd")}.sorted
    val dir2Files = SmvHDFS.dirList(d2).filter{f => f.endsWith(".edd")}.sorted

    require(dir1Files == dir2Files)

    dir1Files.map{fname => 
      val f1 = s"${d1}/${fname}"
      val f2 = s"${d2}/${fname}"
      val title = s"Comparing ${fname} in ${d1} and ${d2}"
      val (pass, log) = compareFiles(f1, f2)
      (title, pass, log)
    }
  }

  def compareDirsReport(d1: String, d2: String) = {
    compareDirs(d1, d2).foreach{case (title, pass, log) => 
      println(title)
      if (!pass) {
        println("EDDs do not match")
        println(log)
      }
    }
  }
}
