from smv import *

from org.tresamigos.smvtest.hashtest import input

class M(SmvModule, SmvOutput):
    def requiresDS(self):
        return [input.table1]

    def run(self, i):
        return i[input.table1]
