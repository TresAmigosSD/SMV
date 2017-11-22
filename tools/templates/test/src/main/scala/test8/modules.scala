package integration.test.test8

import org.tresamigos.smv._

object M2 extends SmvModule("") with SmvOutput {
  override def requiresDS = Seq(SmvExtModuleLink("integration.test.test8_1.modules.M1"))

  override def run(i: runParams) = {
    i(SmvExtModuleLink("integration.test.test8_1.modules.M1"))
  }
}
