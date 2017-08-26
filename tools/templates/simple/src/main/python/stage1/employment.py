import smv
import pyspark.sql.functions as F

__all__ = ['EmploymentByState']

class Employment(smv.SmvCsvFile):
    def path(self):
        return "employment/CB1200CZ11.csv"

    def failAtParsingError(self):
        return False

class EmploymentByState(smv.SmvModule, smv.SmvOutput):
    """Python ETL Example: employ by state"""

    def requiresDS(self):
        return [Employment]

    def run(self, i):
        df = i[Employment]
        return df.groupBy(F.col("ST")).agg(F.sum(F.col("EMP")).alias("EMP"))
