// create the init object "i" rather than create initialization at top level
// because shell would launch a separate command for each evalutaion which
// slows down startup considerably.
// keeping object name short to make the contents easy to access.
object i {
  import org.apache.spark.sql._, functions._
  import org.tresamigos.smv._

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
    implicit val ca = CsvAttributes.defaultCsv
    sqlContext.csvFileWithSchema(fullPath)
  }
  def save(srdd: DataFrame, fullPath: String) = {
    srdd.saveAsCsvWithSchema(fullPath)(CsvAttributes.defaultCsvWithHeader)
  }

  // TODO: rename this
  def check(ds: SmvDataSet) = i.s(ds).edd.addBaseTasks().dump

  private def sourceAppShellInit = {

  }
}

import i._
