package org.tresamigos.smv.class_loader

import org.eclipse.jetty.server.Server
import org.tresamigos.smv.{SmvConfig, SparkTestUtil}

class ClassLoaderTest extends SparkTestUtil {
  val classLoaderTestDir = testDataDir + "ClassLoaderTest"

  private def makeSmvConfig(host: String, port: Integer, classDir: String) = {
    new SmvConfig(Seq("--smv-props",
      s"smv.class_server.host=${host}",
      s"smv.class_server.port=${port}",
      s"smv.class_server.class_dir=${classDir}",
      "-m", "mod1"))
  }

  test("test remote class loader server") {
    var server : Server = null
    val smvConfig = makeSmvConfig("localhost", 9999, classLoaderTestDir)

    try {
      server = new ClassLoaderServer(smvConfig).start()
      val cl = SmvClassLoader(smvConfig)
      // Foo has a dependency on Bar so loading Foo should also load Bar!!!
      val foo = cl.loadClass("org.tresamigos.smv.Foo")
      val bar = foo.getSuperclass
      assert(foo.getClassLoader === cl)
      assert(foo.getName === "org.tresamigos.smv.Foo")
      assert(bar.getClassLoader === cl)
      assert(bar.getName === "org.tresamigos.smv.Bar")

      // TODO: load a standard class e.g. java.lang.Integer

    } finally {
      if (server != null)
        server.stop()
    }
  }

  test("test local client class loader") {
    // TODO: add test here!
  }

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

  test("test load of invalid class name") {
    // TODO: add testcase for invalid class name!!!
  }

  test("test class loader server not started") {
    // TODO: add testcase to test that client doesn't hang if server was not started.
  }
}
