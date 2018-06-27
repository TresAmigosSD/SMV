import smv

class M2(smv.SmvModule, smv.SmvOutput):
    def requiresDS(self):
        return [ smv.smvdataset.SmvExtDataSet("integration.test.test6.M1") ]

    def run(self, i):
        return i[ smv.smvdataset.SmvExtDataSet("integration.test.test6.M1") ]
