package org.tresamigos.smv

class SmvReflectionTest extends SparkTestUtil {
  sparkTest("Test SmvReflection.objectsInPackage method.") {
    object testApp extends SmvApp(testAppArgs.singleStage ++ Seq("-m", "None"), Some(sc)) {}

    val mods: Seq[SmvModule] = SmvReflection.objectsInPackage[SmvModule]("org.tresamigos.smv.smvAppTestPkg1")

    assertUnorderedSeqEqual(mods,
      Seq(org.tresamigos.smv.smvAppTestPkg1.X, org.tresamigos.smv.smvAppTestPkg1.Y))(
        Ordering.by[SmvModule, String](_.name))


    // TODO: CLEANUP: this doesn't belong here
    assert(testApp.moduleNameForPrint(org.tresamigos.smv.smvAppTestPkg1.X) === "X")
  }

}
