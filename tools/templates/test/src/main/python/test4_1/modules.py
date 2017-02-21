from smv import *

from _PROJ_CLASS_.test4_1 import input

class M1(SmvPyModule, SmvPyOutput):
    def requiresDS(self):
        return [input.table]

    def run(self, i):
        return i[input.table]
