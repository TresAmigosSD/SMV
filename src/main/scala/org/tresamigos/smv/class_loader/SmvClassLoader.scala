package org.tresamigos.smv.class_loader

/**
 * Custom "network" class loader that will load classes from remote class loader server (or local).
 * This will enable the loading of modified class files without the need to rebuild the app.
 */
class SmvClassLoader(client: ClassLoaderClientInterface) extends ClassLoader(getClass.getClassLoader) {

  /**
   * Override the default findClass in ClassLoader to load the class using the class loader client.
   * Depending on which client we have (remote/local), this may connect to server or just search locally dir.
   *
   * Note: by the time findClass is called, the loadClass method would have checked the parent class loader
   * first and only defer to findClass if the parent did not have the class!!!
   */
  override def findClass(classFQN: String) : Class[_] = {
    println("findClass: " + classFQN)
    // TODO: handle errors
    val klassBytes = client.getClassBytes(classFQN)
    val klass = defineClass(classFQN, klassBytes, 0, klassBytes.length)
    klass
  }
}

object SmvClassLoader {
  /**
   * Creates the appropriate class loader depending on config.  Can be one of:
   * * Default class loader (if there is not host/dir config)
   * * SmvClassLoader with a remote client connection (if host is specified)
   * * SmvClassLoader with a local client connection (if host is not specified, but class dir is)
   */
  def apply(config: ClassLoaderConfig) : ClassLoader = {
    if (! config.host.isEmpty) {
      println("SmvClassLoader: create remote loader")
      null // must use remote client
    } else if (! config.classDir.isEmpty) {
      println("SmvClassLoader: create local loader")
      null // must use local client
    } else {
      println("SmvClassLoader: create default loader")
      getClass.getClassLoader
    }
  }
}
