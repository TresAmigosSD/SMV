package org.tresamigos.smvtest.stage2.input

import org.tresamigos.smv._

import org.tresamigos.smvtest.stage1._

object EmploymentStateLink extends SmvModuleLink(EmploymentByState)

/** Can link to a Python module */
object EmploymentStateLink2 extends SmvExtModuleLink(
  "org.tresamigos.smvtest.stage1.employment.PythonEmploymentByState")
