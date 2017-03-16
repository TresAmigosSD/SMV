from smv import *
from pyspark.sql.functions import col, sum, lit

from stage2 import inputdata

__all__ = ['EmploymentByStateCategory']

class EmploymentByStateCategory(SmvModule, SmvOutput):
    """Python ETL Example: employment by state with category"""

    def requiresDS(self):
        return [inputdata.EmploymentByStateLink]

    def run(self, i):
        df = i[inputdata.EmploymentByStateLink]
        return df.smvSelectPlus((col("EMP") > lit(1000000)).alias("cat_high_emp"))
