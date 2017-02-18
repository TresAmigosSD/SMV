from smv import *

class M2(SmvPyModule, SmvPyOutput):
    def requiresDS(self):
        return [ SmvPyExtDataSet("_PROJ_CLASS_.test6.M1") ]

    def run(self, i):
        return i[ SmvPyExtDataSet("_PROJ_CLASS_.test6.M1") ]
