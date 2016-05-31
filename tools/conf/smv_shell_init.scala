import org.apache.spark.sql.functions._
import org.tresamigos.smv._, shell._, smvfuncs._

sc.setLogLevel("ERROR")

// create the init object "i" rather than create initialization at top level
// because shell would launch a separate command for each evalutaion which
// slows down startup considerably.
// keeping object name short to make the contents easy to access.
SmvApp.init(Seq("-m", "None").toArray, Option(sc), Option(sqlContext))

object i {
  import org.apache.spark._
  import org.apache.spark.sql.DataFrame
  import org.apache.spark.rdd.RDD
  import java.io.{File, PrintWriter}

  val app = SmvApp.app

  //-------- some helpful functions
  def smvSchema(df: DataFrame) = SmvSchema.fromDataFrame(df)

  // same syntax as one would write in the run() method in SmvModule
  // makes it easy to transfer code to shell and back
  def apply(ds: SmvDataSet) = df(ds)

  def df(ds: SmvDataSet) = {
    app.resolveRDD(ds)
  }

  def hotdeployIfCapable(cl: ClassLoader = getClass.getClassLoader): Unit = {
    import scala.reflect.runtime.universe

    val mir = universe.runtimeMirror(cl).reflect(sc)
    val meth = mir.symbol.typeSignature.member(universe.newTermName("hotdeploy"))

    if (meth.isMethod)
      mir.reflectMethod(meth.asMethod)()
    else
      println("hotdeploy is not available in the current SparkContext")
  }

  def ddf(fqn: String) = {
    val cl = getClass.getClassLoader
    hotdeployIfCapable(cl)
    app.dynamicResolveRDD(fqn, cl)
  }

  /*for existing SmvDataSet (loaded through the fat-jar or previously loaded
    dynamically) a version of `ddf` which takes SmvDataSet as parameter */
  def ddf(ds: SmvDataSet): DataFrame = ddf(ds.getClass.getName)

  // deprecated, should use df instead!!!
  def s(ds: SmvDataSet) = df(ds)

  /** open file using full path */
  def open(path: String, ca: CsvAttributes = null, parserCheck: Boolean = false) ={
    /** isFullPath = true to avoid prepending data_dir */
    object file extends SmvCsvFile(path, ca, null, true) {
      override val forceParserCheck = false
      override val failAtParsingError = parserCheck
    }
    file.rdd
  }

  def openTable(tableName: String) = {
    new SmvHiveTable(tableName).rdd()
  }

  implicit class ShellSrddHelper(df: DataFrame) {
    def sv(path: String) = {
      // TODO: why are we creating SmvDFHelper explicitly here?
      var helper = new org.tresamigos.smv.SmvDFHelper(df)
      helper.saveAsCsvWithSchema(path, CsvAttributes.defaultCsvWithHeader)
    }

    def svl(path: String) = {
      var res = df.collect.map{r => r.mkString(",")}.mkString("\n")
      val pw = new PrintWriter(new File(path))
      pw.println(res)
      pw.close()
    }
  }

  implicit class ShellRddHelper(rdd: RDD[String]) {
    def savel(path: String) = {
      var res = rdd.collect.mkString("\n")
      val pw = new PrintWriter(new File(path))
      pw.println(res)
      pw.close()
    }
  }

  def discoverSchema(path: String, n: Int = 100000, ca: CsvAttributes = CsvAttributes.defaultCsvWithHeader) = {
    implicit val csvAttributes=ca
    val helper = new SchemaDiscoveryHelper(sqlContext)
    val schema = helper.discoverSchemaFromFile(path, n)
    val outpath = SmvSchema.dataPathToSchemaPath(path) + ".toBeReviewed"
    val outFileName = (new File(outpath)).getName
    schema.saveToLocalFile(outFileName)
  }

  // TODO: this should just be a direct helper on ds as it is probably common.
  def dumpEdd(ds: SmvDataSet) = i.s(ds).edd.summary().eddShow

  def compEdds(f1: String, f2: String) = println(EddCompare.compareFiles(f1, f2))
  def compEddDirs(d1: String, d2: String) = EddCompare.compareDirsReport(d1, d2)

}

import i._
