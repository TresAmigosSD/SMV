import smv
import pyspark.sql.functions as F

from stage2 import inputdata

__all__ = ['EmploymentByStateCategory']

class EmploymentByStateCategory(smv.SmvModule, smv.SmvOutput):
    """Python ETL Example: employment by state with category"""

    def requiresDS(self):
        return [inputdata.EmploymentByStateLink]

    def run(self, i):
        df = i[inputdata.EmploymentByStateLink]
        return df.smvSelectPlus((F.col("EMP") > F.lit(1000000)).alias("cat_high_emp"))
