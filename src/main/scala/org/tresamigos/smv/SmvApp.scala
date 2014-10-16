package org.tresamigos.smv

import org.apache.spark.sql.{SchemaRDD, SQLContext}
import org.apache.spark.{SparkContext, SparkConf}

/**
 * Wrapper around a persistence point.  All input/output files in the application
 * (including intermediate steps from modules) must have a corresponding SmvFile instance.
 * This is true even if the intermedidate step does *NOT* need to be persisted.  The
 * nickname will be used to link modules together.
 */
case class SmvFile(nickName: String, basePath: String, csvAttributes: CsvAttributes)

/**
 * defines the module interface.  Each module must declare the file nicknames it will need
 * as input and the file nicknames it will produce.  The module should *not* persist
 * any SRDD.
 * The run method will be called with a sequence of SRDDs that correspond to the
 * list of required nicknames.  It is expected to produce a sequence of SRDDs that
 * correspond to the list of "provides" nicknames.
 */
trait SmvModule {
  def requires() : Seq[String]
  def provides() : Seq[String]
  def run(inputs: Map[String, SchemaRDD]) : Map[String, SchemaRDD]
}

/**
 * Driver for SMV applications.  An application needs to override the getFiles and getModules
 * methods that provide the list of ALL known SmvFiles (in/out) and the list of modules to run.
 */
abstract class SmvApp (val appName: String, _sc: Option[SparkContext] = None) {
  private val dataDir = sys.env.getOrElse("DATA_DIR", "/DATA_DIR_ENV_NOT_SET")
  val conf = new SparkConf().setAppName(appName)
  val sc = _sc.getOrElse(new SparkContext(conf))
  val sqlContext = new SQLContext(sc)

  def getFiles(): Seq[SmvFile]
  def getModules(): Seq[SmvModule]

  lazy private val allFilesByName = getFiles.map(f => (f.nickName, f)).toMap

  /**
   * Transform sequence of required nicknames in given module to a map from nickname
   * to input SRDD.
   */
  private def getRequiredSRDDs(module: SmvModule) : Map[String, SchemaRDD] = {
    module.requires().map { n =>
      val smvFile = allFilesByName(n)
      implicit val ca = smvFile.csvAttributes
      (n, sqlContext.csvFileWithSchema(dataDir + "/" + smvFile.basePath))
    }.toMap
  }

  def getRddByName(rddName: String) = {
    val smvFile = allFilesByName(rddName)
    implicit val ca = smvFile.csvAttributes
    sqlContext.csvFileWithSchema(dataDir + "/" + smvFile.basePath)
  }

  /**
   * Persist the output SRDDs from the given module.
   */
  private def saveProvidedSRDDs(module: SmvModule, outSRDDs: Map[String, SchemaRDD]) = {
    val outNames = module.provides()
    require(outNames.size == outSRDDs.size)

    outNames.foreach { outName =>
      val outFile = allFilesByName(outName)
      val srdd = outSRDDs(outName)
      srdd.saveAsCsvWithSchema(dataDir + "/" + outFile.basePath)(outFile.csvAttributes)
    }
  }

  /**
   * Runs all the provided modules in sequence with no regard to dependency (for now).
   */
  def runAll() = {
    getModules.foreach { m =>
      saveProvidedSRDDs(m, m.run(getRequiredSRDDs(m)))
    }
  }
}

