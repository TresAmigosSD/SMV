from smv import *

class PythonEmployment(SmvPyCsvFile):
    def path(self):
        return "input/employment/CB1200CZ11.csv"

    def failAtParsingError(self):
        return False

    def dqm(self):
        return self.smv._jvm.SmvDQM.apply().add(self.smv._jvm.FailParserCountPolicy(10), self.smv._jvm.scala.Option.apply(True))
