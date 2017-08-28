import smv

from integration.test.test2 import input

class M1(smv.SmvModule):
    def requiresDS(self):
        return [input.table]

    def run(self, i):
        return i[input.table]

class M2(smv.SmvModule, smv.SmvOutput):
    def requiresDS(self):
        return [M1]

    def run(self, i):
        return i[M1]
