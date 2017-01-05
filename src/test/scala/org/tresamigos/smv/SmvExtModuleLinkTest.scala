package org.tresamigos.smv

class SmvExtModuleLinkTest extends SmvTestUtil {
  override val appArgs = Seq("--smv-props", "smv.stages=s1:s2", "-m", "None")

  test("The parent stage of an external module link should be the parent stage of its target") {
    SmvExtModuleLink("link:s2.A").parentStage.name shouldBe "s2"
  }
}
