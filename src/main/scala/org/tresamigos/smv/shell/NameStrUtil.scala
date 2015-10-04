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

import org.tresamigos.smv._

private[smv] object NameStrUtil {
  val wrapLen = 12
  def wrapStr(s: String): String = {
    s.grouped(wrapLen).mkString("\n")
  }

  def dsStr(pkg: SmvPackageManager, ds: SmvDataSet): String = {
    val dstype = ds match {
      case d: SmvOutput => "(O) "
      case d: SmvModuleLink => "(L) "
      case d: SmvFile => "(F) "
      case d: SmvModule => "(M) "
      case d => throw new IllegalArgumentException(s"unknown type of ${d}")
    }

    dstype + pkg.datasetBaseName(ds)
  }

  def dsStrWrap(pkg: SmvPackageManager, ds: SmvDataSet): String = wrapStr(dsStr(pkg, ds))
}
