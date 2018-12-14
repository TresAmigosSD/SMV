import smv

from integration.test.hashtest import input

class M(smv.SmvModule, smv.SmvOutput):
    def requiresDS(self):
        return [input.table1]

    def run(self, i):
        return i[input.table1]
