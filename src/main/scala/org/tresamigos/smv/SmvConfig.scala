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

import scala.util.Try

import java.io.{IOException, InputStreamReader, FileInputStream, File}
import java.util.Properties

import org.rogach.scallop.ScallopConf

/**
 * command line argument parsing using scallop library.
 * See (https://github.com/scallop/scallop) for details on using scallop.
 */
private[smv] class CmdLineArgsConf(args: Seq[String]) extends ScallopConf(args) {
  val DEFAULT_SMV_APP_CONF_FILE  = "conf/smv-app-conf.props"
  val DEFAULT_SMV_USER_CONF_FILE = "conf/smv-user-conf.props"

  banner("""Usage: smv-pyrun -m ModuleToRun
           |Usage: smv-pyrun --run-app
           |""".stripMargin)
  footer("\nFor additional usage information, please refer to the user guide and API docs at: \nhttp://tresamigossd.github.io/SMV")

  val smvProps = propsLong[String]("smv-props", "key=value command line props override")
  val smvAppDir =
    opt("smv-app-dir", noshort = true, default = Some("."), descr = "SMV app directory")

  val cbsPort =
    opt[Int]("cbs-port", noshort = true, default = None, descr = "python callback server port")

  val smvAppConfFile = opt("smv-app-conf",
                           noshort = true,
                           default = Some(DEFAULT_SMV_APP_CONF_FILE),
                           descr = "app level (static) SMV configuration file path")
  val smvUserConfFile = opt("smv-user-conf",
                            noshort = true,
                            default = Some(DEFAULT_SMV_USER_CONF_FILE),
                            descr = "user level (dynamic) SMV configuration file path")
  val publish = opt[String]("publish",
                            noshort = true,
                            default = None,
                            descr = "publish the given modules/stage/app as given version")

  val forceRunAll = toggle("force-run-all",
                        noshort = true,
                        default = Some(false),
                        descrYes = "ignore persisted data and force all modules to run")

  val publishJDBC = toggle("publish-jdbc",
                        noshort = true,
                        default = Some(false),
                        descrYes = "publish the given modules/stage/app through JDBC connection")

  val printDeadModules = toggle("dead",
                        noshort = true,
                        default = Some(false),
                        descrYes = "print a list of the dead modules in this application")

  val publishHive = toggle(
    "publish-hive",
    noshort = true,
    default = Some(false),
    descrYes = "publish|export given modules/stage/app to hive tables",
    descrNo = "Do not publish results to hive tables."
  )

  val exportCsv = opt[String](
    "export-csv",
    noshort = true,
    default = None,
    descr = "publish|export given modules/stage/app to a CSV file at the given path on the local file system"
  )

  val compareEdd = opt[List[String]]("edd-compare",
                                     noshort = true,
                                     default = None,
                                     descr = "compare two edd result files")
  val genEdd = toggle(
    "edd",
    default = Some(false),
    noshort = true,
    descrYes = "summarize data and generate an edd file in the same directory as csv and schema",
    descrNo = "do not summarize data"
  )
  val graph = toggle(
    "graph",
    default = Some(false),
    descrYes = "generate a dot dependency graph of the given modules (modules are not run)",
    descrNo = "do not generate a dot dependency graph"
  )
  val jsonGraph = toggle(
    "json-graph",
    default = Some(false),
    descrYes = "generate a json dependency graph of the given modules (modules are not run)",
    descrNo = "do not generate a json dependency graph"
  )

  val runConfObj = opt[String]("run-conf-obj",
                               noshort = true,
                               default = None,
                               descr = "load and instantiate the configuration object by its fqn")

  // --- data directories override
  val dataDir =
    opt[String]("data-dir", noshort = true, descr = "specify the top level data directory")
  val inputDir = opt[String]("input-dir",
                             noshort = true,
                             descr = "specify the input directory (default: datadir/input")
  val outputDir = opt[String]("output-dir",
                              noshort = true,
                              descr = "specify the output directory (default: datadir/output")
  val historyDir = opt[String]("history-dir",
                              noshort = true,
                              descr = "specify the history directory (default: datadir/history")
  val publishDir = opt[String]("publish-dir",
                               noshort = true,
                               descr = "specify the publish directory (default: datadir/publish")

  val purgeOldOutput = toggle("purge-old-output",
                              noshort = true,
                              default = Some(false),
                              descrYes = "remove all old output files in output dir ")
  val modsToRun = opt[List[String]]("run-module",
                                    'm',
                                    default = Some(Nil),
                                    descr = "run specified list of module FQNs")
  val stagesToRun = opt[List[String]]("run-stage",
                                      's',
                                      default = Some(Nil),
                                      descr = "run all output modules in specified stages")
  val runAllApp = toggle("run-app",
                         default = Some(false),
                         descrYes = "run all output modules in all stages in app.")

  val dryRun = toggle("dry-run",
                      noshort = true,
                      default = Some(false),
                      descrYes = "determine which modules do not have persisted data and will need to be run.")

  // if user specified "edd-compare" command line, user should have supplied two file names.
  validateOpt(compareEdd) {
    case (Some(edds)) if edds.length != 2 => Left("edd-compare param requires two EDD file names")
    case _                                => Right(Unit)
  }

  // override default error message handler that does a sys.exit(1) which makes it difficult to debug.
  printedName = "SmvApp"
  errorMessageHandler = { errMsg =>
    println(printedName + ": " + errMsg)
    throw new SmvRuntimeException(errMsg)
  }

}

/**
 * Container of all SMV config driven elements (cmd line, app props, user props, etc).
 */
class SmvConfig(cmdLineArgs: Seq[String]) {
  import java.nio.file.Paths
  import SmvConfig._

  /*pathJoin has the following behavior:
   *pathJoin("/a/b", "c/d") -> "/a/b/c/d"
   *pathJoin("/a/b", "/c/d") -> "/c/d"
   */
  private def pathJoin(p1: String, p2: String) = Paths.get(p1).resolve(p2).toString

  val DEFAULT_SMV_HOME_CONF_FILE = sys.env.getOrElse("HOME", "") + "/.smv/smv-user-conf.props"

  // check for deprecated DATA_DIR environment variable.
  sys.env.get("DATA_DIR").foreach { d =>
    println(
      "WARNING: use of DATA_DIR environment variable is deprecated. use smv.dataDir instead!!!")
  }

  val cmdLine = new CmdLineArgsConf(cmdLineArgs)

  private def appConfProps  = _loadProps(pathJoin(cmdLine.smvAppDir(), cmdLine.smvAppConfFile()))
  private def usrConfProps  = _loadProps(pathJoin(cmdLine.smvAppDir(), cmdLine.smvUserConfFile()))
  private def homeConfProps = _loadProps(DEFAULT_SMV_HOME_CONF_FILE)
  private val cmdLineProps  = cmdLine.smvProps
  private val defaultProps = Map(
    "smv.appName"     -> "Smv Application",
    "smv.appId"       -> java.util.UUID.randomUUID.toString,
    "smv.stages"      -> "",
    "smv.config.keys" -> "",
    "smv.class_dir"   -> "./target/classes"
  )

  // ---------- Dynamic Run Config Parameters key/values ----------
  var dynamicRunConfig: Map[String, String] = Map.empty

  // computeMergedProps() is used as a cache to avoid expensive IO if appDir is unchanged
  private[smv] def mergedProps = { computeMergedProps() ++ dynamicRunConfig }

  // default the app dir to the cmdLine value. WARNING: this will be changed from smvApp by
  // setRunCofig which will change all of these props dynamically.
  var appConfPath: String = cmdLine.smvAppDir()
  // used as a simple cache flag
  private var lastAppConfPath: String = ""

  private var lastMergedConf: Map[String,String] = Map.empty

  /**
   * This function is implemented as a stupid-simple cache to avoid doing IO to obtain props every time
   */
  private def computeMergedProps(): Map[String,String] = {
    if (appConfPath == lastAppConfPath) return lastMergedConf;
    // update the last value used to determine cache miss
    lastAppConfPath = appConfPath;
    lastMergedConf = defaultProps ++ appConfProps ++ homeConfProps ++ usrConfProps ++ cmdLineProps
    lastMergedConf
  }

  // --- static app config params.  App should access configs through vals below rather than from props maps
  val appName    = mergedProps("smv.appName")
  val appId      = mergedProps("smv.appId")

  // --- stage names are a dynamic prop
  private[smv] def stageNames = { splitProp("smv.stages").toSeq }

  val classDir = mergedProps("smv.class_dir")

  def stageVersions =
    stageNames
      .map { sn: String =>
        {
          val baseName            = FQN.extractBaseName(sn)
          val stageBasePropPrefix = s"smv.stages.${baseName}"
          val stageFQNPropPrefix  = s"smv.stages.${sn}"

          // get stage version (if any)
          val version = getProp(stageBasePropPrefix + ".version").orElse(
            getProp(stageFQNPropPrefix + ".version")
          )

          (sn, version)
        }
      }
      .collect { case (x, Some(y)) => (x, y) }
      .toMap

  val sparkSqlProps = mergedProps.filterKeys(k => k.startsWith("spark.sql."))

  def jdbcUrl: String =
    mergedProps.get("smv.jdbc.url") match {
      case Some(url) =>
        url
      case _ =>
        throw new SmvRuntimeException("JDBC url not specified in SMV config")
    }

  /** The FQN of configuration object for a particular run.  See github issue #319 */
  val runConfObj: Option[String] = cmdLine.runConfObj.get.orElse(mergedProps.get(RunConfObjKey))

  // ---------- User Run Config Parameters key/values ----------
  /** Get user run config parameter as a string. */
  def getRunConfig(key: String): String = dynamicRunConfig.getOrElse(key, mergedProps("smv.config." + key).trim)

  /** Get all run config keys. */
  def getRunConfigKeys(): Seq[String] = splitProp("smv.config.keys") ++ dynamicRunConfig.keySet

  /** compute hash of all key values defined in the app. */
  def getRunConfigHash(): Int = getRunConfigKeys().map(getRunConfig(_)).mkString(":").hashCode()

  // ---------- hierarchy of data / input / output directories

  // TODO: need to remove requirement that data dir is specified (need to start using input dir and if both inpput/output are specified, we dont need datadir
  /** the configured data dir (command line OR props) */
  def dataDir: String = {
    cmdLine.dataDir.get
      .orElse(mergedProps.get("smv.dataDir"))
      .orElse(sys.env.get("DATA_DIR"))
      .getOrElse(throw new SmvRuntimeException(
        "Must specify a data-dir either on command line or in conf."))
  }

  def inputDir: String = {
    cmdLine.inputDir.get.orElse(mergedProps.get("smv.inputDir")).getOrElse(dataDir + "/input")
  }

  def outputDir: String = {
    cmdLine.outputDir.get.orElse(mergedProps.get("smv.outputDir")).getOrElse(dataDir + "/output")
  }

  def historyDir: String = {
    cmdLine.historyDir.get.orElse(mergedProps.get("smv.historyDir")).getOrElse(dataDir + "/history")
  }

  def publishDir: String = {
    cmdLine.publishDir.get
      .orElse(mergedProps.get("smv.publishDir"))
      .getOrElse(dataDir + "/publish")
  }

  /**
   * load the given properties file and return the resulting props as a scala Map.
   * Note: if the provided file does not exit, this will just silently return an empty map.
   * This is by design as the default app/user smv config files may be missing (valid).
   * Adopted from the Spark AbstractCommnadBuilder::loadPropertiesFile
   */
  private def _loadProps(propsFileName: String): scala.collection.Map[String, String] = {
    import scala.collection.JavaConverters._

    val props     = new Properties()
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
            case e: IOException => None
          }
        }
      }
    }

    props.asScala
  }

  /**
   * convert the property value to an array of strings.
   * Return empty array if original prop value was an empty string
   */
  private[smv] def splitProp(propName: String) = {
    val propVal = mergedProps(propName)
    if (propVal == "")
      Array[String]()
    else
      propVal.split(Array(',', ':')).map(_.trim)
  }

  /** Return the value of given configuration parameter as an optional string. */
  def getProp(propName: String): Option[String] = {
    mergedProps.get(propName)
  }

  /** Get the given configuration property value as an integer option. */
  private[smv] def getPropAsInt(propName: String): Option[Int] = {
    mergedProps.get(propName).flatMap { s: String =>
      Try(s.toInt).toOption
    }
  }
}

object SmvConfig {
  val RunConfObjKey: String = "smv.runConfObj"
}
