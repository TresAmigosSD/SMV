package org.tresamigos.smv

class SmvExtModuleLinkTest extends SmvTestUtil {
  override val appArgs = Seq("--smv-props", "smv.stages=s1:s2", "-m", "None")

  // SmvExtModuleLink is no longer a full module - it is for declarative purposes only
  // This test should be updated to deal with links to SmvExtModulePython or otherwise
  // be deleted
  ignore("The parent stage of an external module link should be the parent stage of its target") {
    val extLink: SmvModuleLink = SmvExtModuleLink("s2.A")
    extLink.smvModule.parentStage shouldBe "s2"
  }
}
