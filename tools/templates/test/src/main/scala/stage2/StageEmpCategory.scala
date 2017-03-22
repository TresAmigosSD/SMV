package org.tresamigos.smvtest.stage2

import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql._

import org.tresamigos.smv._

/**
 * Assign a category to state employment numbers.
 */
object StageEmpCategory extends SmvModule("Employment By Stage with Category") with SmvOutput {

  override def requiresDS() = Seq(input.EmploymentStateLink);

  override def run(i: runParams) = {
    val df = i(input.EmploymentStateLink)
    import df.sqlContext.implicits._

    df.smvSelectPlus(
      $"EMP" >  lit(1000000) as "cat_high_emp"
    )
  }
}

object StageEmpCategory2 extends SmvModule("Stage2 example: use link to external dataset") with SmvOutput {

  override def requiresDS() = Seq(input.EmploymentStateLink2);

  override def run(i: runParams) = {
    val df = i(input.EmploymentStateLink2)
    import df.sqlContext.implicits._

    df.smvSelectPlus(
      $"EMP" >  lit(1000000) as "cat_high_emp"
    )
  }
}

object StageEmpCategory3 extends SmvModule("Stage2 example: depend on an external link") with SmvOutput {
  val externalLink = SmvExtModuleLink("org.tresamigos.smvtest.stage1.employment.PythonEmploymentByState")

  override def requiresDS() = Seq(externalLink);

  override def run(i: runParams) = {
    val df = i(externalLink)
    import df.sqlContext.implicits._

    df.smvSelectPlus(
      $"EMP" >  lit(1000000) as "cat_high_emp"
    )
  }
}
