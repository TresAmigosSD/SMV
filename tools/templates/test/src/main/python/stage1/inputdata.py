from smv import *
from smv.dqm import *

class PythonEmployment(SmvCsvFile):
    def path(self):
        return "input/employment/CB1200CZ11.csv"

    def failAtParsingError(self):
        return False

    def dqm(self):
        """An example DQM policy"""
        return SmvDQM().add(FailParserCountPolicy(10))
