from smv import *

from integration.test.test4 import input

class M2(SmvModule, SmvOutput):
    def requiresDS(self):
        return [input.M1Link]

    def run(self, i):
        return i[input.M1Link]
