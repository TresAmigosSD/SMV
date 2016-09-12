package _PROJ_CLASS_.stage2

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

    df.selectPlus(
      $"EMP" >  lit(1000000) as "cat_high_emp"
    )
  }
}
