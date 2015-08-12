package org.tresamigos.smv

import scala.reflect.runtime.{universe => ru}
import scala.util.Try

/**
 * helper methods for module reflection/discovery
 */
object SmvReflection {
  private val mirror = ru.runtimeMirror(this.getClass.getClassLoader)

  /** maps the FQN of module name to the module object instance. */
  private[smv] def moduleNameToObject(modName: String) = {
    mirror.reflectModule(mirror.staticModule(modName)).instance.asInstanceOf[SmvModule]
  }

  /** extract instances (objects) in given package that implement SmvModule. */
  private[smv] def modulesInPackage(pkgName: String): Seq[SmvModule] = {
    import com.google.common.reflect.ClassPath
    import scala.collection.JavaConversions._

    ClassPath.from(this.getClass.getClassLoader).
      getTopLevelClasses(pkgName).
      map(c => Try(moduleNameToObject(c.getName))).
      filter(_.isSuccess).
      map(_.get).
      toSeq
  }
}
