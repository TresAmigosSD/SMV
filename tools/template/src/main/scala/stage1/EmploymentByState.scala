package _PROJ_CLASS_.stage1

import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql._

import org.tresamigos.smv._

/**
 * ETL Module Example
 */
object EmploymentByState extends SmvModule("ETL Example: Employment") with SmvOutput {

  override def requiresDS() = Seq(input.employment);

  override def run(i: runParams) = {
    val df = i(input.employment)

    df.groupBy("ST").agg(
      sum("EMP") as "EMP"
    )
  }
}
