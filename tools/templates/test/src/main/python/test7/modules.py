from smv import *

class M2(SmvModule, SmvOutput):
    def requiresDS(self):
        return [ SmvExtModuleLink("integration.test.test7_1.M1") ]

    def run(self, i):
        return i[ SmvExtModuleLink("integration.test.test7_1.M1") ]
