from smv import SmvPyModuleLink, SmvPyExtDataSet
from _PROJ_CLASS_.stage1 import employment as emp

class EmploymentByStateLink(SmvPyModuleLink):
    """Example: how to use SmvPyModuleLink"""
    def target(self):
        return emp.PythonEmploymentByState

class EmploymentByStateLink2(SmvPyModuleLink):
    """Example: linking to an external dataset"""
    def target(self):
        return SmvPyExtDataSet("com.my.stage1.EmploymentByState")
