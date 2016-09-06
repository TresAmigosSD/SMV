from smv import *

class InputZipCounty(SmvPyCsvFile):
    def __init__(self, smv):
        super(InputZipCounty, self).__init__(smv, "pdda_raw/00_geo/zip_to_county.csv")

class PyZipPrimaryCounty(SmvPyModule):
    def requiresDS(self):
        return [InputZipCounty(self.smv)]

    def compute(self):
        super(PyZipPrimaryCounty, self).compute()
        df = self.smv.pymods['test.InputZipCounty']
        df.peek()
        df.show()
        return df
