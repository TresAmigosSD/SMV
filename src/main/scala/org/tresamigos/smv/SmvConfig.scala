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

import java.io.{IOException, InputStreamReader, FileInputStream, File}
import java.util.Properties

import org.rogach.scallop.ScallopConf


/**
 * command line argumetn parsing using scallop library.
 * See (https://github.com/scallop/scallop) for details on using scallop.
 */
private[smv] class CmdLineArgsConf(args: Seq[String]) extends ScallopConf(args) {
  val DEFAULT_SMV_APP_CONF_FILE = "conf/smv-app-conf"
  val DEFAULT_SMV_USER_CONF_FILE = "conf/smv-user-conf"

  val smvProps = propsLong[String]("smv-props", "key=value command line props override")
  val smvAppConfFile = opt("smv-app-conf", noshort = true,
    default = Some(DEFAULT_SMV_APP_CONF_FILE),
    descr = "app level (static) SMV configuration file path")
  val smvUserConfFile = opt("smv-user-conf", noshort = true,
    default = Some(DEFAULT_SMV_USER_CONF_FILE),
    descr = "user level (dynamic) SMV configuration file path")
  val devMode = toggle("dev", default=Some(false),
    descrYes="enable dev mode (persist all intermediate module results",
    descrNo="enable production mode (all modules are evaluated from scratch")
  val graph = toggle("graph", default=Some(false),
    descrYes="generate a dependency graph of the given modules (modules are not run)",
    descrNo="do not generate a dependency graph")
  val json = toggle("json", default=Some(false),
    descrYes="generate a json object to represent entire app's module dependency (modules are not run)",
    descrNo="do not generate a json")
  // TODO: rename this to "edddir" as it is not a generic output dir and add as config prop.
  // TODO: probably need to rethink if this should be a dir or just a flag and edd goes along with csv/schema files (as meta data)
  val eddDir = opt[String]("outdir",
    descr = "if provided, dumps the module's edd to the specified output directory")

  val modules = trailArg[List[String]](descr="FQN of modules to run/graph")
}


/**
 * Container of all SMV config driven elements (cmd line, app props, user props, etc).
 */
class SmvConfig(cmdLineArgs: Seq[String]) {
  val cmdLineArgsConf = new CmdLineArgsConf(cmdLineArgs)

  private val appConfProps = _loadProps(cmdLineArgsConf.smvAppConfFile())
  private val usrConfProps = _loadProps(cmdLineArgsConf.smvUserConfFile())
  private val cmdLineProps = cmdLineArgsConf.smvProps
  private val defaultProps = Map(
    "smv.appName" -> "Smv Application",
    "smv.stages" -> ""
  )

  // merge order is important here.  Highest priority comes last as it will ride all previous
  private[smv] val mergedProps = defaultProps ++ appConfProps ++ usrConfProps ++ cmdLineProps

  // --- config params.  App should access configs through vals below rather than from props maps
  val appName = mergedProps("smv.appName")
  val stages = mergedProps("smv.stages") // TODO: should be an array of SmvStage objects.
  /**
   * load the given properties file and return the resulting props as a scala Map.
   * Note: if the provided file does not exit, this will just silently return an empty map.
   * This is by design as the default app/user smv config files may be missing (valid).
   * Adopted from the Spark AbstractCommnadBuilder::loadPropertiesFile
   */
  private def _loadProps(propsFileName: String) : scala.collection.Map[String, String] = {
    import scala.collection.JavaConverters._

    val props = new Properties()
    val propsFile = new File(propsFileName)

    if (propsFile.isFile()) {
      var fd: FileInputStream = null
      try {
        fd = new FileInputStream(propsFile)
        props.load(new InputStreamReader(fd, "UTF-8"));
      } finally {
        if (fd != null) {
          try {
            fd.close()
          } catch {
            case e:IOException => None
          }
        }
      }
    }

    props.asScala
  }
}

