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

import java.io.{PrintWriter, File}

import org.apache.log4j.{LogManager, Logger, Level}
import org.apache.spark.SparkContext
import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.sql.hive.test.TestHive
import org.scalatest._

trait SparkTestUtil extends FunSuite with BeforeAndAfterAll with Matchers {
  var sc: SparkContext       = _
  var sqlContext: SQLContext = _

  def disableLogging = false

  def name() = this.getClass().getName().filterNot(_ == '$')

  /** top of data dir to be used by tests.  For per testcase temp directory, use testcaseTempDir instead */
  final val testDataDir = {
    val fullpath = getClass.getResource("/data").getPath()
    fullpath.substring(fullpath.lastIndexOf("target/")) + "/"
  }

  /**
   * Creates a local spark context, and cleans
   * it up even if your test fails.  Also marks the test with the tag SparkTest, so you can
   * turn it off
   *
   * By default, it turn off spark logging, b/c it just clutters up the test output.  However,
   * when you are actively debugging one test, you may want to turn the logs on
   *
   * Source: http://blog.quantifind.com/posts/spark-unit-test/
   *
   * One slight variation of the above, instead of capturing the log level for certain loggers
   * and reseting them after every test, this runner will ALWAYS set the log level to ERROR
   * for ALL current registered loggers.  If the user wants to enable logging at a lower level,
   * they can call "SparkTestUtil.setLoggingLevel" with the lower level.  This can even be
   * used by non-sparkTest test cases.
   */
  override def beforeAll() = {
    super.beforeAll()
    if (disableLogging)
      SparkTestUtil.setLoggingLevel(Level.OFF)
    else
      SparkTestUtil.setLoggingLevel(Level.ERROR)

    sqlContext = TestHive
    sc = sqlContext.sparkContext
    sqlContext.setConf("spark.sql.shuffle.partitions", "4")
    resetTestcaseTempDir()
  }

  override def afterAll() = {
    sqlContext = null
    sc = null
    System.clearProperty("spark.master.port")
    // re-enable normal logging for next test if we disabled logging here.
    if (disableLogging) SparkTestUtil.setLoggingLevel(Level.ERROR)
    super.afterAll()
  }

  /** With BeforeAndAfterAll, sparkTest method is simply a wrapper of test method
   *  Drop this method, and use test method in the suites

  def sparkTest(name: String)(body: => Unit) {
    test(name) {
      body
    }
  }
   **/
  /** name of a scratch test directory specific to this test case. */
  def testcaseTempDir = testDataDir + this.getClass.getName

  /** wipe out the temp test directory and recreate an empty instance. */
  def resetTestcaseTempDir() = {
    SmvHDFS.deleteFile(testcaseTempDir)
    new File(testcaseTempDir).mkdir()
  }

  /** create a temp file in the test case temp dir with the given contents. */
  def createTempFile(baseName: String, fileContents: String = "xxx"): File = {
    val outFile = new File(testcaseTempDir, baseName)
    val pw      = new PrintWriter(outFile)
    pw.write(fileContents)
    pw.close
    outFile
  }

  /**
   * Ensure that the given expected and actual result double sequences are "equal".  Equality is checked
   * against the given epsilon margin of error to account for floating point precision errors.
   */
  def assertDoubleSeqEqual(resultSeq: Seq[Any], expectSeq: Seq[Double], epsilon: Double = 0.01) {
    import java.lang.Math.abs
    assert(resultSeq.length === expectSeq.length)
    resultSeq
      .map {
        case d: Double => d
        case i: Int    => i.toDouble
        case l: Long   => l.toDouble
        case f: Float  => f.toDouble
        case _         => Double.MinValue
      }
      .zip(expectSeq)
      .foreach {
        case (a, b) => assert(abs(a - b) < epsilon, s"because array element $a not equal $b")
      }
  }

  /**
   * Ensure that two arbitrary sequences are equal regardless of the order of items in the sequence
   */
  def assertUnorderedSeqEqual[T: Ordering](resultSeq: Seq[T], expectSeq: Seq[T]) {
    assert(resultSeq.length === expectSeq.length)

    val sortedResSeq = resultSeq.sorted
    val sortedExpSeq = expectSeq.sorted

    sortedResSeq.zip(sortedExpSeq).foreach {
      case (a, b) => assert(a == b, s"because array element $a not equal $b")
    }
  }

  /**
   * Verify that the data in the df matches the expected result strings.
   * The expectedRes is assumed to be a set of lines separated by ";"
   * The order of the result strings is not important.
   */
  def assertSrddDataEqual(df: DataFrame, expectedRes: String) = {
    val resLines      = df.collect.map(_.toString.stripPrefix("[").stripSuffix("]"))
    val expectedLines = expectedRes.split(";").map(_.trim)
    assertUnorderedSeqEqual(resLines, expectedLines)
  }

  def assertDataFramesEqual(df1: DataFrame, df2: DataFrame) = {
    var mergedDF = (df1.collect, df2.collect).zipped
    assert(
      mergedDF
        .flatMap((r1, r2) => (r1.toSeq, r2.toSeq).zipped.map((f1, f2) => (f1 == f2)))
        .reduce(_ && _))
  }

  /**
   * validates that the schema of the given SRDD matches the schema defined by
   * the schemaStr parameter.  The schemaStr parameter is jsut a ";" list of
   * schema entries.
   */
  def assertSrddSchemaEqual(df: DataFrame, schemaStr: String) = {
    val expSchema = SmvSchema.fromString(schemaStr)
    val resSchema = SmvSchema.fromDataFrame(df)
    assert(resSchema.toString === expSchema.toString)
  }

  /**
   * Check whether a string matches a Regex
   **/
  def assertStrMatches(haystack: String, needle: scala.util.matching.Regex) = {
    assert(needle.findFirstIn(haystack) != None, s"because $haystack does not match $needle")
  }

  def assertStrIgnoreSpace(s1: String, s2: String) = {
    def rmsp(s: String) = """[ \t]+""".r.replaceAllIn(s, "")
    assert(rmsp(s1) === rmsp(s2))
  }

  /**
   * Verify that the contents of the file at the given path equal the expected contents.
   */
  def assertFileEqual(filePath: String, expContent: String) = {
    val res = SmvHDFS.readFromFile(filePath)
    assert(res === expContent)
  }

  def assertTextContains(text: String, expContent: String) = {
    expContent.split('\n').foreach { l =>
      assert(text.split('\n').contains(l), s"${l} is not in the text")
    }
  }
}

object SparkTestUtil {
  def setLoggingLevel(level: Level) = {
    Logger.getLogger("org").setLevel(level)
    Logger.getLogger("akka").setLevel(level)
  }
}

/**
 * Use SmvTestUtil when you need to access a default SmvApp.app
 * User can override the `appArgs` method to specify the `app` in the SmvApp object
 * {{{
 * class MySmvTest extends SmvTestUtil {
 *   override def appArgs = Seq("-m", "MyModule", "--data-dir", testcaseTempDir)
 *   test("test MyModule ...."){
 *      ...
 *   }
 * }
 * }}}
 */
trait SmvTestUtil extends SparkTestUtil {

  /** appArgs could be overridden by concrete class to initiate SmvApp.app as required */
  def appArgs: Seq[String] = Seq(
    "-m",
    "None",
    "--data-dir",
    testcaseTempDir
  )
  var app: SmvApp = _

  override def beforeAll() = {
    super.beforeAll()
    SmvApp.init(appArgs.toArray, Option(sc), Option(sqlContext))
    app = SmvApp.app
  }

  override def afterAll() = {
    app = null
    super.afterAll()
  }

  def open(path: String) = {
    val file = SmvCsvFile("./" + path, CsvAttributes.defaultCsv)
    file.rdd()
  }

  def dfFrom(schemaStr: String, data: String): DataFrame = app.createDF(schemaStr, data)

  def load(mod: SmvDataSet): SmvDataSet         = app.dsm.load(mod.urn).head
  def loadM(mods: SmvDataSet*): Seq[SmvDataSet] = app.dsm.load(mods.map(_.urn): _*)
}

/** Base trait for unit tests that do not need a Spark test environment */
trait SmvUnitSpec extends FlatSpec with BeforeAndAfterEach with BeforeAndAfterAll with Matchers
