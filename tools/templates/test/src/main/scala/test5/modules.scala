package _PROJ_CLASS_.test5

import org.tresamigos.smv._

object M2 extends SmvModule("") with SmvOutput {
  override def requiresDS = Seq( SmvExtModule("_PROJ_CLASS_.test5.modules.M1") )

  override def run(i: runParams) = {
    i( SmvExtModule("_PROJ_CLASS_.test5.modules.M1") )
  }
}
