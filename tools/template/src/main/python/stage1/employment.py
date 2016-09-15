from smv import *
from pyspark.sql.functions import col, sum, lit

from _PROJ_CLASS_.stage1 import inputdata

__all__ = ['PythonEmploymentByState']

class PythonEmploymentByState(SmvPyModule):
    def description(self):
        return "Python ETL Example: Employment"

    def requiresDS(self):
        return [inputdata.PythonEmployment]

    def run(self, i):
        df = i[inputdata.PythonEmployment]
        return df.groupBy(col("ST")).agg(sum(col("EMP")).alias("EMP"))


class PythonEmploymentByStateCategory(SmvPyModule):
    def description(self):
        return "Python Employment By State With Category"

    def requiresDS(self):
        return [PythonEmploymentByState]

    def run(self, i):
        df = i[PythonEmploymentByState]
        return df.selectPlus((col("EMP") > lit(1000000)).alias("cat_high_emp"))
