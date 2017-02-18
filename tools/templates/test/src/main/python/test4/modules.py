from smv import *

from _PROJ_CLASS_.test4 import input

class M2(SmvPyModule, SmvPyOutput):
    def requiresDS(self):
        return [input.M1Link]

    def run(self, i):
        return i[input.M1Link]
