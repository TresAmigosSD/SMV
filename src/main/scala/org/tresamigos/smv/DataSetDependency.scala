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

package org.tresamigos.smv

import java.io.{StringWriter, PrintWriter, InputStream}

import scala.tools.asm.{ClassReader, ClassVisitor, MethodVisitor, Opcodes, util}
import scala.collection.mutable.{Set => MSet}

/**
 * Using asm to figure out SmvDataSet dependency
 *
 * Client code
 * {{{
 * val c=DataSetDependency(com.mycomp.MyModule.getClass.getName)
 * println(c.dependsDS)
 * println(c.dependsAnc)
 * }}}
 *
 * TODO: should be smv-private when the code is stable
 **/
case class DataSetDependency(className: String) {
  // convert com.foo.bar to "/com/foo/bar.class"
  private val classResourcePath = "/" + className.replace('.', '/') + ".class"

  lazy val (dependsDS, dependsAnc) = {
    val ds: MSet[String] = MSet()
    val anc: MSet[String] = MSet()

    val is: InputStream = getClass.getResourceAsStream(classResourcePath)
    val cv = new DsVisitor(Opcodes.ASM4, null, ds, anc)

    val reader=new ClassReader(is)
    reader.accept(cv, ClassReader.SKIP_DEBUG)

    (ds.toSeq, anc.toSeq)
  }

  private class DsVisitor(api: Int, cv: ClassVisitor, ds: MSet[String], anc: MSet[String])
    extends ClassVisitor(api, cv) {

    override def visitMethod(access: Int, name: String, desc: String, signature: String, exceptions: Array[String]) = {
      if (Seq("requiresDS", "requiresAnc", "version").contains(name)) null
      else new RunVisitor(api, null, ds, anc)
    }
  }

  private class RunVisitor(api: Int, mv: MethodVisitor, ds: MSet[String], anc: MSet[String])
    extends MethodVisitor(api, mv) {

    private def addModule(rawName: String) = {
        val cName = rawName.replaceAll("""package\$""", "").replace('/', '.').replaceAll("""\$""", "")
        if (cName != className.replaceAll("""\$""", "")) {
          if(SmvReflection.findObjectByName[SmvDataSet](cName).isSuccess) ds += cName
          if(SmvReflection.findObjectByName[SmvAncillary](cName).isSuccess) anc += cName
        }
        Unit
    }

    override def visitFieldInsn(opc: Int, owner: String, name: String, desc: String) = {
      if (opc == Opcodes.GETSTATIC) {
        addModule(owner)
      }
    }

    override def visitMethodInsn(opc: Int, owner: String, name: String, desc: String) = {
      if (opc == Opcodes.INVOKEVIRTUAL) {
        addModule(owner)
      }
    }
  }

 /**
  * The ByteCode as a string
  *
  * The asm framework is easy to modify. The following code will print out the entire code,
  * which will be useful for adding new function to `DataSetDependency`
  * Please refer http://download.forge.objectweb.org/asm/asm4-guide.pdf for using asm
  **/
  def bcode() = {
    val is: InputStream = getClass.getResourceAsStream(classResourcePath)

    val stringWriter = new StringWriter()
    val printWriter = new PrintWriter(stringWriter)
    val traceClassVisitor = new util.TraceClassVisitor(null, new util.Textifier(), printWriter)

    val reader=new ClassReader(is)
    reader.accept(traceClassVisitor, ClassReader.SKIP_DEBUG)
    stringWriter.toString()
  }
}

/** rule violation report */
case class DependencyViolation(description: String, components: Seq[SmvDataSet])

sealed trait DependencyRule {

  /** Convenience method to convert a list of offending datasets to Violation instances */
  def toViolation(results: Seq[SmvDataSet]): Option[DependencyViolation] =
    if (results.isEmpty) None else Some(DependencyViolation(description, results))

  def description: String

  /** Returns None if the rule is followed, Some(violations) if violated */
  def check(ds: SmvDataSet): Option[DependencyViolation]

}

object SameStageDependency extends DependencyRule {
  override val description = "Dependency modules must be in the same stage"

  override def check(ds: SmvDataSet) = {
    val diff = for {
      dep <- ds.requiresDS
      // External module links have their stage inferred from the
      // target link fqn.  The name for the link module itself is not
      // included in the urn.  So we won't be able to infer the stage
      // of the external link module.  Hence the exclusion from this test.
      if !dep.isInstanceOf[SmvExtModuleLink] && dep.parentStage != ds.parentStage
    } yield dep

    toViolation(diff)
  }
}

object LinkFromDiffStage extends DependencyRule {
  override val description = "Module Links must come from another stage"

  override def check(ds: SmvDataSet) = {
    val links = for {
      dep <- ds.requiresDS
      if dep.isInstanceOf[SmvModuleLink] &&
      dep.asInstanceOf[SmvModuleLink].smvModule.parentStage == ds.parentStage
    } yield dep

    toViolation(links)
  }
}
