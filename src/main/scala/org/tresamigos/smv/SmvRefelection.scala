package org.tresamigos.smv

import scala.reflect.ClassTag
import scala.reflect.runtime.{universe => ru}
import scala.util.Try

/**
 * helper methods for module reflection/discovery
 */
object SmvReflection {
  private val mirror = ru.runtimeMirror(this.getClass.getClassLoader)

  /** maps the FQN of a scala object to the actual object instance. */
  private[smv] def objectNameToInstance[T: ClassTag](objName: String) : T = {
    val ct = implicitly[ClassTag[T]]
    mirror.reflectModule(mirror.staticModule(objName)).instance match {
      case ct(t) => t
      case _ => throw new ClassCastException("can not cast: " + objName)
    }
  }

  /** extract instances (objects) in given package that implement type T. */
  private[smv] def objectsInPackage[T: ClassTag](pkgName: String): Seq[T] = {
    import com.google.common.reflect.ClassPath
    import scala.collection.JavaConversions._

    ClassPath.from(this.getClass.getClassLoader).
      getTopLevelClasses(pkgName).
      map(c => Try(objectNameToInstance[T](c.getName))).
      filter(t => t.isSuccess).
      map(_.get).
      toSeq
  }
}
