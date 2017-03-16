from smv import SmvModuleLink, SmvPyExtDataSet
from org.tresamigos.smvtest.stage1 import employment as emp

EmploymentByStateLink = SmvModuleLink(emp.PythonEmploymentByState)

EmploymentByStateLink2 = SmvModuleLink(SmvPyExtDataSet("org.tresamigos.smvtest.stage1.EmploymentByState"))
