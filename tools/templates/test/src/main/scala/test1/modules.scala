package org.tresamigos.smvtest.test1

import org.tresamigos.smv._

object M1 extends SmvModule("") with SmvOutput {
  override def requiresDS = Seq(input.table)

  override def run(i: runParams) = {
    i(input.table)
  }
}

object M2 extends SmvModule("") with SmvOutput {
  override def requiresDS = Seq(M1)

  override def run(i: runParams) = {
    i(M1)
  }
}
