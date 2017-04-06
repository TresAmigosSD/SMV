package org.tresamigos.smvtest.test8

import org.tresamigos.smv._

object M2 extends SmvModule("") with SmvOutput {
  override def requiresDS = Seq(SmvExtModuleLink("org.tresamigos.smvtest.test8_1.modules.M1"))

  override def run(i: runParams) = {
    i(SmvExtModuleLink("org.tresamigos.smvtest.test8_1.modules.M1"))
  }
}
