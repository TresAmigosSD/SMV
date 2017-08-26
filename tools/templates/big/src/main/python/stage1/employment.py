import smv
import pyspark.sql.functions as F

__all__ = ['EmploymentByState']

from _PKG_NAME_ import _DEP_NAME_

class _MOD_NAME_(smv.SmvModule, smv.SmvOutput):
    """Python ETL Example: employ by state"""

    def requiresDS(self):
        return [_DEP_NAME_]

    def run(self, i):
        df = i[_DEP_NAME_]
        return df.groupBy(F.col("ST")).agg(F.sum(F.col("EMP")).alias("EMP"))
