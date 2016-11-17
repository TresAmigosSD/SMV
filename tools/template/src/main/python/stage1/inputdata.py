from smv import *

class PythonEmployment(SmvPyCsvFile):
    def path(self):
        return "input/employment/CB1200CZ11.csv"

    def failAtParsingError(self):
        return False

    def dqm(self):
        """An example DQM policy"""
        return self.smvDQM().add(self.FailParserCountPolicy(10), self.smvapp.scalaOption(True))
