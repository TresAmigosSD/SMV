from smv import *

class M2(SmvModule, SmvOutput):
    def requiresDS(self):
        return [ SmvPyExtDataSet("org.tresamigos.smvtest.test6.M1") ]

    def run(self, i):
        return i[ SmvPyExtDataSet("org.tresamigos.smvtest.test6.M1") ]
