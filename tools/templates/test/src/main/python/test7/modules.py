from smv import *

class M2(SmvPyModule, SmvPyOutput):
    def requiresDS(self):
        return [ SmvPyExtModuleLink("org.tresamigos.smvtest.test7_1.M1") ]

    def run(self, i):
        return i[ SmvPyExtModuleLink("org.tresamigos.smvtest.test7_1.M1") ]
