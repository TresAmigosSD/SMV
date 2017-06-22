package integration.test.test3

import org.tresamigos.smv._

object M2 extends SmvModule("") with SmvOutput {

  override def requiresDS = Seq(M1Link)

  override def run(i: runParams) = {
    i(M1Link)
  }
}
