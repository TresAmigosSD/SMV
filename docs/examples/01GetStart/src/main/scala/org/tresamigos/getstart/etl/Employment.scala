package org.tresamigos.getstart.etl

import org.tresamigos.getstart.core._
import org.tresamigos.smv._

/**
 * ETL Module Example
 */

object EmploymentRaw extends SmvModule("ETL Example: Employment") {

  override def version() = 0;
  override def requiresDS() = Seq(ExampleApp.employment);
 
  override def run(i: runParams) = {
    val srdd = i(ExampleApp.employment)
    import srdd.sqlContext.implicits._
    
    srdd.select(
      "EMP"
      )
  }
}
