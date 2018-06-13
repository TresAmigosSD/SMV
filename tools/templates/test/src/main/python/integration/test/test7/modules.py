import smv

class M2(smv.SmvModule, smv.SmvOutput):
    def requiresDS(self):
        return [ smv.SmvExtModuleLink("integration.test.test7_1.M1") ]

    def run(self, i):
        return i[ smv.SmvExtModuleLink("integration.test.test7_1.M1") ]
