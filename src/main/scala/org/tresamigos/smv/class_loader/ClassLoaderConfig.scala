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

package org.tresamigos.smv.class_loader

import org.tresamigos.smv.SmvConfig

/**
 * Wrapper around class loader specific configuration.
 * Can be created from command line args or a previously created SmvConfig object.
 */
class ClassLoaderConfig(private val smvConfig: SmvConfig) {

  def this(cmdLineArgs: Seq[String]) = {
    this(new SmvConfig(cmdLineArgs))
  }

  val host = smvConfig.classServerHost
  val port = smvConfig.classServerPort
  val classDir = smvConfig.classServerClassDir
}

