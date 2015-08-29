package _PROJ_CLASS_.stage1.etl

import _PROJ_CLASS_.stage1.input._
import org.tresamigos.smv._

/**
 * ETL Module Example
 */

object EmploymentRaw extends SmvModule("ETL Example: Employment") with SmvOutput {

  override def requiresDS() = Seq(employment);
 
  override def run(i: runParams) = {
    val df = i(employment)

    df.limit(10).select(
      "EMP"
      )
  }
}
