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
import java.util.{Properties, ArrayList}

import org.rogach.scallop.ScallopConf

/**
 * command line argument parsing using scallop library.
 * See (https://github.com/scallop/scallop) for details on using scallop.
 */
private[smv] class CmdLineArgsConf(args: Seq[String]) extends ScallopConf(args) {
  val DEFAULT_SMV_APP_CONF_FILE  = "conf/smv-app-conf.props"
  val DEFAULT_SMV_USER_CONF_FILE = "conf/smv-user-conf.props"

  banner("""Usage: smv-run -m ModuleToRun
           |Usage: smv-run --run-app
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

  // default the app dir to the cmdLine value.
  private var _appDir: String = cmdLine.smvAppDir()

  // getter and setter because we need to recompute merged props when appDir is set explicitly
  def appDir : String = _appDir

  def setAppDir(appDirPath: String): Unit = {
    _appDir = appDirPath
    mergedPropsCache = readAppConf
  }

  private def appConfProps  = _loadProps(pathJoin(_appDir, cmdLine.smvAppConfFile()))
  private def usrConfProps  = _loadProps(pathJoin(_appDir, cmdLine.smvUserConfFile()))
  private def homeConfProps = _loadProps(DEFAULT_SMV_HOME_CONF_FILE)
  private val cmdLineProps  = cmdLine.smvProps
  private val defaultProps = Map(
    "smv.appName"            -> "Smv Application",
    "smv.appId"              -> java.util.UUID.randomUUID.toString,
    "smv.stages"             -> "",
    "smv.config.keys"        -> "",
    "smv.class_dir"          -> "./target/classes",
    "smv.user_libraries"     -> "",
    "smv.maxCbsPortRetries"  -> "10"
  )

  // ---------- Dynamic Run Config Parameters key/values ----------
  var dynamicRunConfig: Map[String, String] = Map.empty

  // initially, the merged props cache should do the io to read app conf
  private var mergedPropsCache: Map[String,String] = readAppConf

  private[smv] def mergedProps = { mergedPropsCache ++ dynamicRunConfig }

  // calling this performs the file io, hence the name
  private def readAppConf: Map[String, String] = {
    val confFromFiles = appConfProps ++ homeConfProps ++ usrConfProps

    // merge the project props over defaults but lose to cmdLine
    defaultProps ++ confFromFiles ++ cmdLineProps
  }

  def quickRun = mergedProps.getOrElse("smv.quickRun", false)

  // --- App should access configs through vals below rather than from props maps
  def appName           = mergedProps("smv.appName")
  def appId             = mergedProps("smv.appId")
  def maxCbsPortRetries = mergedProps("smv.maxCbsPortRetries")

  // --- stage names are a dynamic prop
  private[smv] def stageNames = { splitProp("smv.stages").toSeq }

  // --- user libraries are dynamic as well
  private[smv] def userLibs = { splitProp("smv.user_libraries").toSeq }

  val sparkSqlProps = mergedProps.filterKeys(k => k.startsWith("spark.sql."))

  def jdbcUrl: String =
    mergedProps.get("smv.jdbc.url") match {
      case Some(url) =>
        url
      case _ =>
        throw new SmvRuntimeException("JDBC url not specified in SMV config")
    }
  
  def jdbcDriver: String = mergedProps.get("smv.jdbc.driver") match {
    case None => throw new SmvRuntimeException("JDBC driver is not specified in SMV config")
    case Some(ret) => ret
  }

  // ---------- User Run Config Parameters key/values ----------
  def getRunConfig(key: String): String = dynamicRunConfig.getOrElse(key, getRunConfigFromConf(key))

  def getRunConfigFromConf(key: String): String  = {
    val runConfig = mergedProps.getOrElse("smv.config." + key, null);
    if (runConfig == null) return null
    return runConfig.trim
  }

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

/** Scaffolding: for python side to test passing in configs.
 *  Target interface for future SmvConfig class
 **/
class SmvConfig2(
  val modsToRun: ArrayList[String],
  val stagesToRun: ArrayList[String],
  val cmdLine: java.util.Map[String, String],
  var props: java.util.Map[String, String],
  var dataDirs: java.util.Map[String, String]
){
  def printall() = {
    println(modsToRun)
    println(stagesToRun)
    println(cmdLine)
    println(props)
    println(dataDirs)
  }

  def reset(
    _props: java.util.Map[String, String], 
    _dataDirs: java.util.Map[String, String]
  ) {
    props = _props
    dataDirs = _dataDirs
  }
}