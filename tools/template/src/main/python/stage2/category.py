from smv import *
from pyspark.sql.functions import col, sum, lit

from _PROJ_CLASS_.stage2 import inputdata

__all__ = ['PythonEmploymentByStateCategory']

class PythonEmploymentByStateCategory(SmvPyModule, SmvPyOutput):
    """Python ETL Example: employment by state with category"""

    def requiresDS(self):
        return [inputdata.EmploymentByStateLink]

    def run(self, i):
        df = i[inputdata.EmploymentByStateLink]
        return df.smvSelectPlus((col("EMP") > lit(1000000)).alias("cat_high_emp"))

class PythonEmploymentByStateCategory2(SmvPyModule, SmvPyOutput):
    """Python ETL Example: depending on Scala module
    """

    ScalaMod = SmvPyExtDataSet("__PROJ_CLASS_.stage1.EmploymentByState")

    def requiresDS(self):
        return [self.ScalaMod]

    def run(self, i):
        df = i[self.ScalaMod]
        return df.smvSelectPlus((col("EMP") > lit(1000000)).alias("cat_high_emp"))

class PythonEmploymentByStateCategory3(SmvPyModule, SmvPyOutput):
    """Python ETL Example: link to a Scala module"""

    def requiresDS(self):
        return [inputdata.EmploymentByStateLink2]

    def run(self, i):
        df = i[inputdata.EmploymentByStateLink2]
        return df.smvSelectPlus((col("EMP") > lit(1000000)).alias("cat_high_emp"))

class PythonEmploymentByStateCategory4(SmvPyModule, SmvPyOutput):
    """Python ETL Example: depends on a Scala module that's a link itself"""

    ScalaModLink = SmvPyExtDataSet("_PROJ_CLASS_.stage2.input.EmploymentStateLink")

    def requiresDS(self):
        return [self.ScalaModLink]

    def run(self, i):
        df = i[self.ScalaModLink]
        return df.smvSelectPlus((col("EMP") > lit(1000000)).alias("cat_high_emp"))
