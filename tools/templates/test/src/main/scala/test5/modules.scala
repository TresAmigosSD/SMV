package integration.test.test5

import org.tresamigos.smv._

object M2 extends SmvModule("") with SmvOutput {
  override def requiresDS = Seq(SmvExtModule("integration.test.test5.modules.M1"))

  override def run(i: runParams) = {
    i(SmvExtModule("integration.test.test5.modules.M1"))
  }
}
