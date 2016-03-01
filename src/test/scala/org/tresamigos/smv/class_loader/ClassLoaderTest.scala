/*
 * This file is licensed under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.tresamigos.smv.class_loader

import org.eclipse.jetty.server.Server
import org.tresamigos.smv.{SmvTestUtil, SmvConfig, SparkTestUtil}

trait ClassLoaderTestHelper {
  this : SparkTestUtil =>

  val PORT = 9999
  val classLoaderTestDir = testDataDir + "ClassLoaderTest"

  def cmdLineArgs(host: String, port: Integer, classDir: String) = {
    Seq("--smv-props",
      s"smv.class_server.host=${host}",
      s"smv.class_server.port=${port}",
      s"smv.class_server.class_dir=${classDir}",
      "smv.dataDir=.",
      "-m", "mod1")
  }

  def makeSmvConfig(host: String, port: Integer, classDir: String) = {
    new SmvConfig(cmdLineArgs(host, port, classDir))
  }

  def testLoadOfValidClass(classLoader: ClassLoader) = {
    // Foo has a dependency on Bar so loading Foo should also load Bar!!!
    val foo = classLoader.loadClass("com.smv.Foo")
    val bar = foo.getSuperclass
    assert(foo.getClassLoader === classLoader)
    assert(foo.getName === "com.smv.Foo")
    assert(bar.getClassLoader === classLoader)
    assert(bar.getName === "com.smv.Bar")

    // TODO: load a standard class e.g. java.lang.Integer
  }

  def testLoadOfInvalidClass(classLoader: ClassLoader) = {
    intercept[ClassNotFoundException] {
      classLoader.loadClass("com.smv.invalid.Foo")
    }
  }
}

class SmvAppDynamicResolveTest extends SmvTestUtil with ClassLoaderTestHelper {
  // override smvApp args to create an app with local class loader.
  override def appArgs: Seq[String] = cmdLineArgs("", 0, classLoaderTestDir)

  test("test SmvApp.dynamicResolveRDD function") {
    val df = app.dynamicResolveRDD("com.smv.MyModule", getClass.getClassLoader)
    assertSrddDataEqual(df, "1;2;3")
  }
}

class RemoteClassLoaderTest extends SparkTestUtil with ClassLoaderTestHelper {
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

  test("test local class loader valid class") {
    testLoadOfValidClass(classLoader)
  }

  test("test local class loader class not found") {
    testLoadOfInvalidClass(classLoader)
  }
}

class ClassLoaderFactoryTest extends SparkTestUtil with ClassLoaderTestHelper {
  test("test SmvClassLoader client factory for remote server config") {
    // remote server config: (hostname, port, and class dir)
    val clRemoteConfig = makeSmvConfig(".com", 1234, "/tmp")
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
