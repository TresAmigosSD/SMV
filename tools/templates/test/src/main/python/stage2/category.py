from smv import *
from pyspark.sql.functions import col, sum, lit

from org.tresamigos.smvtest.stage2 import inputdata

__all__ = ['PythonEmploymentByStateCategory']

class PythonEmploymentByStateCategory(SmvPyModule, SmvPyOutput):
    """Python ETL Example: employment by state with category"""

    def requiresDS(self):
        return [inputdata.EmploymentByStateLink]

    def run(self, i):
        df = i[inputdata.EmploymentByStateLink]
        return df.smvSelectPlus((col("EMP") > lit(1000000)).alias("cat_high_emp"))

class PythonEmploymentByStateCategory2(SmvPyModule, SmvPyOutput):
    """Python ETL Example: link to a Scala module"""

    def requiresDS(self):
        return [inputdata.EmploymentByStateLink2]

    def run(self, i):
        df = i[inputdata.EmploymentByStateLink2]
        return df.smvSelectPlus((col("EMP") > lit(1000000)).alias("cat_high_emp"))
