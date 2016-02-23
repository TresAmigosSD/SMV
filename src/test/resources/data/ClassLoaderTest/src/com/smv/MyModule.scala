package com.smv
import org.tresamigos.smv._

object MyModule extends SmvModule("desc") {
  override val isEphemeral = true
  override def requiresDS() = Seq()

  override def run(i: runParams) = {
    app.createDF("x:Integer", "1;2;3")
  }
}

