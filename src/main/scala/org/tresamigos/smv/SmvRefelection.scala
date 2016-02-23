package org.tresamigos.smv

import scala.reflect.ClassTag
import scala.reflect.runtime.{universe => ru}
import scala.util.Try


/**
 * helper methods for module reflection/discovery using arbitrary class loader
 */
private[smv] class SmvReflection(private val classLoader: ClassLoader) {
  private val mirror = ru.runtimeMirror(classLoader)

  /** maps the FQN of a scala object to the actual object instance. */
  private[smv] def objectNameToInstance[T: ClassTag](objName: String) : T =
    findObjectByName(objName).get

  /** Does a companion object exist with the given FQN and of the given type? */
  def findObjectByName[T: ClassTag](fqn: String): Try[T] = {
    val ct = implicitly[ClassTag[T]]
    Try {
      mirror.reflectModule(mirror.staticModule(fqn)).instance match {
        case ct(t) => t
        case _ =>  throw new ClassCastException("can not cast: " + fqn)
      }
    }
  }

  /** extract instances (objects) in given package that implement type T. */
  def objectsInPackage[T: ClassTag](pkgName: String): Seq[T] = {
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

/**
 * helper methods for module reflection/discovery using default class loader
 */
private[smv] object SmvReflection {
  private val ref = new SmvReflection(this.getClass.getClassLoader)

  /** maps the FQN of a scala object to the actual object instance. */
  private[smv] def objectNameToInstance[T: ClassTag](objName: String) : T =
    ref.objectNameToInstance(objName)

  /** Does a companion object exist with the given FQN and of the given type? */
  def findObjectByName[T: ClassTag](fqn: String): Try[T] = {
    ref.findObjectByName(fqn)
  }

  /** extract instances (objects) in given package that implement type T. */
  def objectsInPackage[T: ClassTag](pkgName: String): Seq[T] = {
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

