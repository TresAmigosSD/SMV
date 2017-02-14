from smv import *
from pyspark.sql.functions import col, sum, lit

__all__ = ['EmploymentByState']

class Employment(SmvPyCsvFile):
    def path(self):
        return "input/employment/CB1200CZ11.csv"

    def failAtParsingError(self):
        return False

    def dqm(self):
        """An example DQM policy"""
        return self.smvDQM().add(self.FailParserCountPolicy(10), self.smvPy.scalaOption(True))


class EmploymentByState(SmvPyModule, SmvPyOutput):
    """Python ETL Example: employ by state"""

    def requiresDS(self):
        return [Employment]

    def run(self, i):
        df = i[Employment]
        return df.groupBy(col("ST")).agg(sum(col("EMP")).alias("EMP"))
