from smv import *

class M2(SmvModule, SmvOutput):
    def requiresDS(self):
        return [ SmvExtDataSet("integration.test.test6.M1") ]

    def run(self, i):
        return i[ SmvExtDataSet("integration.test.test6.M1") ]
