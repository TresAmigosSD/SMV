import org.apache.spark.sql.functions._
import org.tresamigos.smv._, shell._, smvfuncs._, util.Edd

val sc = spark.sparkContext
sc.setLogLevel("ERROR")

// create the init object "i" rather than create initialization at top level
// because shell would launch a separate command for each evalutaion which
// slows down startup considerably.
// keeping object name short to make the contents easy to access.
SmvApp.init(Array("-m", "None"), Option(spark))

object i {
  import org.apache.spark._
  import org.apache.spark.sql.DataFrame
  import org.apache.spark.rdd.RDD

  val app = SmvApp.app

  // TODO: this should just be a direct helper on ds as it is probably common.
  def dumpEdd(ds: SmvDataSet) = df(ds).edd.summary().eddShow

  def compEdds(f1: String, f2: String) = println(Edd.compareFiles(f1, f2))
  def compEddDirs(d1: String, d2: String) = Edd.compareDirsReport(d1, d2)

}

import i._
