package _PROJ_CLASS_.test1

import org.tresamigos.smv._

object S1 extends SmvModule("") with SmvOutput {
  override def requiresDS = Seq(input.table)

  override def run(i: runParams) = {
    i(input.table)
  }
}

object S2 extends SmvModule("") with SmvOutput {
  override def requiresDS = Seq(S1)

  override def run(i: runParams) = {
    i(S1)
  }
}
