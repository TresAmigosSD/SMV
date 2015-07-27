package org.tresamigos.getstart.core

import org.apache.spark.SparkContext
import org.tresamigos.smv._

/**
 * Driver for all LungCancer applications.
 * The app name is taken as a parameter to allow individual modules to override the app name
 * in their standalone run mode.
 */
class ExampleApp(
                  _args: Seq[String],
                  _sc: Option[SparkContext] = None
                  )
      extends SmvApp(_args, _sc) {

  val num_partitions = sys.env.getOrElse("PARTITIONS", "64")
  sqlContext.setConf("spark.sql.shuffle.partitions", num_partitions)
  
  override val rejectLogger = new SCRejectLogger(sc, 3)
}

object ExampleApp {
  val caBar = new CsvAttributes(delimiter = '|', hasHeader = true)
  
  /********************************************************************
   * Product
   ********************************************************************/
  val employment = SmvCsvFile("input/employment/CB1200CZ11.csv", caBar)
  def main(args: Array[String]) {
    new ExampleApp(args).run()
  }
}

