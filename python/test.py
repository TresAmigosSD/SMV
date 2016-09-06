from smv import *

class PyZipPrimaryCounty(SmvPyModule):
    def requiresDS(self):
        return [SmvPyCsvFile(self.smv, "pdda_raw/00_geo/zip_to_county.csv")]

    def compute(self):
        super(PyZipPrimaryCounty, self).compute()
        df = self.smv.sqlContext.createDataFrame([(1, 'Adam'), (2, 'Eve')], ["id", "name"])
        df.printSchema()
        df.show()
        return df
