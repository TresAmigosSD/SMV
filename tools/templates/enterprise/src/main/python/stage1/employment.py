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
