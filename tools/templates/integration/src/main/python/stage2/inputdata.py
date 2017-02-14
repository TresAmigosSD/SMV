from smv import SmvPyModuleLink, SmvPyExtDataSet
from _PROJ_CLASS_.stage1 import employment as emp

EmploymentByStateLink = SmvPyModuleLink(emp.PythonEmploymentByState)

EmploymentByStateLink2 = SmvPyModuleLink(SmvPyExtDataSet("com.projcom.proj.stage1.EmploymentByState"))
