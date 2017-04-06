package com.smv
import org.tresamigos.smv._

object MyModule extends SmvModule("desc") {
  override val isEphemeral  = true
  override def requiresDS() = Seq()

  override def run(i: runParams) = {
    app.createDF("x:Integer", "1;2;3")
  }
}

object MyModule2 extends SmvModule("desc2") {
  override val isEphemeral = true
  override def requiresDS() = Seq(
    MyModule
  )

  override def run(i: runParams) = {
    val df = i(MyModule)
    df
  }
}
