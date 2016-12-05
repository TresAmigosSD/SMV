from smv import *
from pyspark.sql.functions import col, sum, lit

from _PROJ_CLASS_.stage1 import inputdata

__all__ = ['PythonEmploymentByState']

class PythonEmploymentByState(SmvPyModule, SmvPyOutput):
    """Python ETL Example: employ by state"""

    def requiresDS(self):
        return [inputdata.PythonEmployment]

    def run(self, i):
        df = i[inputdata.PythonEmployment]
        return df.groupBy(col("ST")).agg(sum(col("EMP")).alias("EMP"))


class PythonEmploymentByStateCategory(SmvPyModule, SmvPyOutput):
    """Python ETL Example: employment by state with category"""

    def requiresDS(self):
        return [PythonEmploymentByState]

    def run(self, i):
        df = i[PythonEmploymentByState]
        return df.smvSelectPlus((col("EMP") > lit(1000000)).alias("cat_high_emp"))

class PythonEmploymentByStateCategory2(SmvPyModule, SmvPyOutput):
    """Python ETL Example: depending on Scala module
    """

    ScalaMod = SmvPyExtDataSet("_PROJ_CLASS_.stage1.EmploymentByState")

    def requiresDS(self):
        return [self.ScalaMod]

    def run(self, i):
        df = i[self.ScalaMod]
        return df.smvSelectPlus((col("EMP") > lit(1000000)).alias("cat_high_emp"))
