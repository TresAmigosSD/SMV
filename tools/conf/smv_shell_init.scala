import org.apache.spark.sql.functions._
import org.tresamigos.smv._, shell._, smvfuncs._

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
}

import i._
