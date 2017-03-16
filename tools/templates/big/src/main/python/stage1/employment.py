from smv import *
from pyspark.sql.functions import col, sum, lit

__all__ = ['EmploymentByState']

from _PKG_NAME_ import _DEP_NAME_

class _MOD_NAME_(SmvModule, SmvOutput):
    """Python ETL Example: employ by state"""

    def requiresDS(self):
        return [_DEP_NAME_]

    def run(self, i):
        df = i[_DEP_NAME_]
        return df.groupBy(col("ST")).agg(sum(col("EMP")).alias("EMP"))
