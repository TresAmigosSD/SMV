import smv
import pyspark.sql.functions as F

from stage1 import inputdata

__all__ = ['EmploymentByState']

class EmploymentByState(smv.SmvModule, smv.SmvOutput):
    """Python ETL Example: employ by state"""

    def requiresDS(self):
        return [inputdata.Employment]

    def run(self, i):
        df = i[inputdata.Employment]
        return df.groupBy(F.col("ST")).agg(F.sum(F.col("EMP")).alias("EMP"))
