from smv import *
from pyspark.sql.functions import col, sum, lit

from stage1 import inputdata

__all__ = ['EmploymentByState']

class EmploymentByState(SmvPyModule, SmvPyOutput):
    """Python ETL Example: employ by state"""

    def requiresDS(self):
        return [inputdata.Employment]

    def run(self, i):
        df = i[inputdata.Employment]
        return df.groupBy(col("ST")).agg(sum(col("EMP")).alias("EMP"))
