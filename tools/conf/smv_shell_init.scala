
// create the init object "i" rather than create initialization at top level
// because shell would launch a separate command for each evalutaion which
// slows down startup considerably.
// keeping object name short to make the contents easy to access.
object i {
  import org.apache.spark.sql._, functions._
  import org.apache.spark.rdd.RDD
  import org.tresamigos.smv._
  import java.io.{File, PrintWriter}

  val app = new SmvApp(Seq("-m", "None"), Option(sc))
  val sqlContext = app.sqlContext

  //-------- some helpful functions
  def smvSchema(df: DataFrame) = SmvSchema.fromDataFrame(df)

  def df(ds: SmvDataSet) = {
    ds.injectApp(app)
    app.resolveRDD(ds)
  }

  // deprecated, should use df instead!!!
  def s(ds: SmvDataSet) = df(ds)

  def open(fullPath: String) = {
    implicit val ca = CsvAttributes.defaultCsvWithHeader
    sqlContext.csvFileWithSchema(fullPath)
  }

  implicit class ShellSrddHelper(df: DataFrame) {
    def save(path: String) = {
      // TODO: why are we creating SmvDFHelper explicitly here?
      var helper = new org.tresamigos.smv.SmvDFHelper(df)
      helper.saveAsCsvWithSchema(path)(CsvAttributes.defaultCsvWithHeader)
    }

    def savel(path: String) = {
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

  def discoverSchema(path: String, n: Int = 100000) = {
    implicit val ca=CsvAttributes.defaultCsvWithHeader
    val file=sqlContext.csvFileWithSchemaDiscovery(path, n)
    val schema=SmvSchema.fromDataFrame(file)
    val outpath = path + ".schema"
    schema.saveToFile(sc, outpath)
  }

  // TODO: this should just be a direct helper on ds as it is probably common.
  def dumpEdd(ds: SmvDataSet) = i.s(ds).edd.addBaseTasks().dump
}

import i._
