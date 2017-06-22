from smv import *

from integration.test.hashtest import input

class M(SmvModule, SmvOutput):
    def requiresDS(self):
        return [input.table1]

    def run(self, i):
        return i[input.table1]
