package org.tresamigos.smv

class SmvReflectionTest extends SparkTestUtil {
//  override def appArgs = testAppArgs.singleStage ++ Seq("-m", "None")

  test("Test SmvReflection.objectsInPackage method.") {
    val mods: Seq[SmvModule] = SmvReflection.objectsInPackage[SmvModule]("org.tresamigos.smv.smvAppTestPkg1")

    assertUnorderedSeqEqual(mods,
      Seq(org.tresamigos.smv.smvAppTestPkg1.X, org.tresamigos.smv.smvAppTestPkg1.Y))(
        Ordering.by[SmvModule, String](_.name))


    // TODO: CLEANUP: this doesn't belong here
//    assert(testApp.moduleNameForPrint(org.tresamigos.smv.smvAppTestPkg1.X) === "X")
  }

}
