package _PROJ_CLASS_.stage2.input

import org.tresamigos.smv._

import _PROJ_CLASS_.stage1._

object EmploymentStateLink extends SmvModuleLink(EmploymentByState)

/** Can link to a Python module */
object EmploymentStateLink2 extends SmvExtModuleLink(
  "_PROJ_CLASS_.stage1.employment.PythonEmploymentByState")
