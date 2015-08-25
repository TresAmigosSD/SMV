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
    val df = i(ExampleApp.employment)
    import df.sqlContext.implicits._
    
    df.select(
      "EMP"
      )
  }
}
