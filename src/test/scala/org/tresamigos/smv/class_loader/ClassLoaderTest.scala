package org.tresamigos.smv.class_loader

import org.eclipse.jetty.server.Server
import org.tresamigos.smv.{SmvConfig, SparkTestUtil}

trait ClassLoaderTestHelper {
  this : SparkTestUtil =>

  val classLoaderTestDir = testDataDir + "ClassLoaderTest"

  def makeSmvConfig(host: String, port: Integer, classDir: String) = {
    new SmvConfig(Seq("--smv-props",
      s"smv.class_server.host=${host}",
      s"smv.class_server.port=${port}",
      s"smv.class_server.class_dir=${classDir}",
      "-m", "mod1"))
  }

  def testLoadOfValidClass(classLoader: ClassLoader) = {
    // Foo has a dependency on Bar so loading Foo should also load Bar!!!
    val foo = classLoader.loadClass("org.tresamigos.smv.Foo")
    val bar = foo.getSuperclass
    assert(foo.getClassLoader === classLoader)
    assert(foo.getName === "org.tresamigos.smv.Foo")
    assert(bar.getClassLoader === classLoader)
    assert(bar.getName === "org.tresamigos.smv.Bar")

    // TODO: load a standard class e.g. java.lang.Integer
  }

  def testLoadOfInvalidClass(classLoader: ClassLoader) = {
    intercept[ClassNotFoundException] {
      classLoader.loadClass("com.smv.invalid.Foo")
    }
  }
}

class RemoteClassLoaderTest extends SparkTestUtil with ClassLoaderTestHelper {
  val PORT = 9999
  var server : Server = _
  var classLoader : ClassLoader = _

  override def beforeAll() = {
    super.beforeAll()
    val smvConfig = makeSmvConfig("localhost", PORT, classLoaderTestDir)
    server = new ClassLoaderServer(smvConfig).start()
    classLoader = SmvClassLoader(smvConfig)
  }

  override def afterAll() = {
    server.stop()
    super.afterAll()
  }

  test("test remote class loader server valid class") {
    testLoadOfValidClass(classLoader)
  }

  test("test remote class loader server class not found") {
    testLoadOfInvalidClass(classLoader)
  }

  test("test class loader server not started") {
    // TODO: add testcase to test that client doesn't hang if server was not started.
  }
}

class LocalClassLoaderTest extends SparkTestUtil with ClassLoaderTestHelper {
  var classLoader : ClassLoader = _

  override def beforeAll() = {
    super.beforeAll()
    val smvConfig = makeSmvConfig("", 0, classLoaderTestDir)
    classLoader = SmvClassLoader(smvConfig)
  }

  test("test local class loader server valid class") {
    testLoadOfValidClass(classLoader)
  }

  test("test local class loader server class not found") {
    testLoadOfInvalidClass(classLoader)
  }
}

class ClassLoaderFactoryTest extends SparkTestUtil with ClassLoaderTestHelper {
  test("test SmvClassLoader client factory for remote server config") {
    // remote server config: (hostname, port, and class dir)
    val clRemoteConfig = makeSmvConfig("foo.com", 1234, "/tmp")
    val clRemote = SmvClassLoader(clRemoteConfig).asInstanceOf[SmvClassLoader]

    // we better have created a remote client connection.
    assert(clRemote.client.isInstanceOf[ClassLoaderClient])
  }

  test("test SmvClassLoader client factory for local client config") {
    // local server config: no hostname but a class dir (port is ignored)
    val clLocalConfig = makeSmvConfig("", 1234, "/tmp")
    val clLocal = SmvClassLoader(clLocalConfig).asInstanceOf[SmvClassLoader]

    // we better have created a local client connection.
    assert(clLocal.client.isInstanceOf[LocalClassLoaderClient])
  }

  test("test SmvClassLoader client factory for default config") {
    // default class loader config: no hostname, no class dir.  Port is ignored
    val clLDefaultConfig = makeSmvConfig("", 1234, "")
    val clDefault = SmvClassLoader(clLDefaultConfig)

    // we better have gotten the standard default jar loader.
    assert(clDefault === getClass.getClassLoader)
  }
}
