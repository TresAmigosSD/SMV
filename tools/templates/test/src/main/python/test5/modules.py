import smv

from integration.test.test5 import input

class M1(smv.SmvModule, smv.SmvOutput):
    def requiresDS(self):
        return [input.table]

    def run(self, i):
        return i[input.table]
