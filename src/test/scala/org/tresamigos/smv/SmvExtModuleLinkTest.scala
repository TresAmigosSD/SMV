package org.tresamigos.smv

class SmvExtModuleLinkTest extends SmvTestUtil {
  override val appArgs = Seq("--smv-props", "smv.stages=s1:s2", "-m", "None")

  test("The parent stage of an external module link should be the parent stage of its target") {
    val extLink: SmvModuleLink = SmvExtModuleLink("s2.A")
    extLink.smvModule.parentStage shouldBe app.stages.findStage("s2")
  }

  // This test should be updated after the refactor
  ignore("External modules dependent on links should observe link stage rule") {
    val extMod = new SmvExtModule("s2.A") {
      //override def requiresDS = Seq(SmvExtModuleLink("s1.L"))
    }

    app.checkDependencyRules(extMod) shouldBe 'Empty
  }
}
