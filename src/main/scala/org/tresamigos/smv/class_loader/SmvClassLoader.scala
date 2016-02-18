package org.tresamigos.smv.class_loader

import org.tresamigos.smv.SmvConfig

/**
 * Custom "network" class loader that will load classes from remote class loader server (or local).
 * This will enable the loading of modified class files without the need to rebuild the app.
 */
class SmvClassLoader(val client: ClassLoaderClientInterface) extends ClassLoader(getClass.getClassLoader) {

  // TODO: need to make this a parallel loader!!!
  // See URLClassLoader for example usage of "ClassLoader.registerAsParallelCapable()"

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

  override def loadClass(classFQN: String) : Class[_] = {
    var c : Class[_] = null
    getClassLoadingLock(classFQN).synchronized {
      c = findLoadedClass(classFQN)
      if (c == null) {
        try {
            c = getParent.loadClass(classFQN)
        } catch {
          case e: ClassNotFoundException => {/* ignore class not found in parent */}
        }

        if (c == null) {
          c = findClass(classFQN)
        }
      }
    }

    c
  }

}

object SmvClassLoader {
  /**
   * Creates the appropriate class loader depending on config.  Can be one of:
   * * Default class loader (if there is not host/dir config)
   * * SmvClassLoader with a remote client connection (if host is specified)
   * * SmvClassLoader with a local client connection (if host is not specified, but class dir is)
   */
  def apply(smvConfig: SmvConfig) : ClassLoader = {
    val clConfig = new ClassLoaderConfig(smvConfig)

    if (! clConfig.host.isEmpty) {
      // network class loader with remote client connection
      new SmvClassLoader(new ClassLoaderClient(clConfig))
    } else if (! clConfig.classDir.isEmpty) {
      // network class loader with local client connection
      new SmvClassLoader(new LocalClassLoaderClient(clConfig))
    } else {
      // default jar class loader
      getClass.getClassLoader
    }
  }
}
