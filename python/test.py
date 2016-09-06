from smv import *
from pyspark.sql.functions import col
from pyspark.sql import DataFrame

class InputZipCounty(SmvPyCsvFile):
    def __init__(self, smv):
        super(InputZipCounty, self).__init__(smv, "pdda_raw/00_geo/zip_to_county.csv")

class PyZipPrimaryCounty(SmvPyModule):
    def requiresDS(self):
        return [InputZipCounty(self.smv)]

    def compute(self):
        super(PyZipPrimaryCounty, self).compute()

        df = self.smv.pymods['test.InputZipCounty']

        filtered = df.select(
            df['ZCTA5'].alias("Zip"),
            df['GEOID'].alias("County"),
            df['POPPT'].alias("Population"),
            df['HUPT'].alias("HouseHold")
        )

        grouped = filtered.smvGroupBy(col("Zip"))
        p1 = DataFrame(grouped.smvTopNRecs(1, smv_copy_array(df._sc, col('Population').desc())), self.smv.sqlContext)
        p2 = p1.select(p1["Zip"], p1["County"].alias("PCounty"))
        return DataFrame(filtered.joinByKey(p2, ["Zip"], "inner"), self.smv.sqlContext)
