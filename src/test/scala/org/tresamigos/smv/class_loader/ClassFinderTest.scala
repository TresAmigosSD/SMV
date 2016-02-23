package org.tresamigos.smv.class_loader

import org.tresamigos.smv.SparkTestUtil

class ClassFinderTest extends SparkTestUtil {
  val classLoaderTestDir = testDataDir + "ClassLoaderTest"
  val finder = new ClassFinder(classLoaderTestDir)

  test("find valid class object") {
    val b = finder.getClassBytes("com.smv.Foo")

    // first 4 bytes of class should be "ca fe ba be"
    assert(b(0) === 0xca.toByte)
    assert(b(1) === 0xfe.toByte)
    assert(b(2) === 0xba.toByte)
    assert(b(3) === 0xbe.toByte)
  }

  test("find invalid class object") {
    val b = finder.getClassBytes("invalid.Foo")
    assert(b === null)
  }
}
