import smv
import smv.dqm as dqm

class Employment(smv.SmvCsvFile):
    def path(self):
        return "employment/CB1200CZ11.csv"

    def failAtParsingError(self):
        return False

    def dqm(self):
        """An example DQM policy"""
        return dqm.SmvDQM().add(dqm.FailParserCountPolicy(10))
