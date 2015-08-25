package _PROJ_CLASS_.stage1.etl

import _PROJ_CLASS_.stage1._
import org.tresamigos.smv._

/**
 * ETL Module Example
 */

object EmploymentRaw extends SmvModule("ETL Example: Employment") {

  override def requiresDS() = Seq(ExampleApp.employment);
 
  override def run(i: runParams) = {
    val df = i(ExampleApp.employment)

    df.limit(10).select(
      "EMP"
      )
  }
}
