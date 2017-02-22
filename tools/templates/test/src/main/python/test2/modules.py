from smv import *

from org.tresamigos.smvtest.test2 import input

class M1(SmvPyModule):
    def requiresDS(self):
        return [input.table]

    def run(self, i):
        return i[input.table]

class M2(SmvPyModule, SmvPyOutput):
    def requiresDS(self):
        return [M1]

    def run(self, i):
        return i[M1]
