package _PROJ_CLASS_.stage2.input

import org.tresamigos.smv._

import _PROJ_CLASS_.stage1._

object EmploymentStateLink extends SmvModuleLink(EmploymentByState)

/** This intermediate object is necessary for explicit dataset hash calculation */
object PythonEmploymentByState extends SmvExtDataSet(
  "_PROJ_CLASS_.stage1.employment.PythonEmploymentByState") with SmvOutput

/** Can link to a Python module */
object EmploymentStateLink2 extends SmvModuleLink(PythonEmploymentByState)
