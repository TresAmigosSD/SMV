from smv import SmvPyModuleLink, SmvPyExtLink
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
        return SmvPyExtLink("_PROJ_CLASS_.stage1.EmploymentByState")
