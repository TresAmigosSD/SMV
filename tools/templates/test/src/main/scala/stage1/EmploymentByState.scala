package org.tresamigos.smvtest.stage1

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

object EmploymentByState2 extends SmvModule("Example: using a Python module") with SmvOutput {
  val dep = SmvExtModule("org.tresamigos.smvtest.stage1.inputdata.PythonEmployment")

  override val requiresDS = Seq(dep)

  override def run(i: runParams) = {
    val df = i(dep)
    df.groupBy("ST").agg(sum("EMP") as "EMP")
  }
}
