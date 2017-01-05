from smv import SmvPyModuleLink, SmvPyExtDataSet
from _PROJ_CLASS_.stage1 import employment as emp

class EmploymentByStateLink(SmvPyModuleLink):
    """Example: how to use SmvPyModuleLink"""
    @classmethod
    def target(cls):
        return emp.PythonEmploymentByState

class EmploymentByStateLink2(SmvPyModuleLink):
    """Example: linking to an external dataset"""
    @classmethod
    def target(cls):
        return SmvPyExtDataSet("_PROJ_CLASS_.stage1.EmploymentByState")
