package org.tresamigos.smvtest.test5

import org.tresamigos.smv._

object M2 extends SmvModule("") with SmvOutput {
  override def requiresDS = Seq(SmvExtModule("org.tresamigos.smvtest.test5.modules.M1"))

  override def run(i: runParams) = {
    i(SmvExtModule("org.tresamigos.smvtest.test5.modules.M1"))
  }
}
