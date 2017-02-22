from smv import SmvPyModuleLink, SmvPyExtDataSet
from org.tresamigos.smvtest.stage1 import employment as emp

EmploymentByStateLink = SmvPyModuleLink(emp.PythonEmploymentByState)

EmploymentByStateLink2 = SmvPyModuleLink(SmvPyExtDataSet("org.tresamigos.smvtest.stage1.EmploymentByState"))
