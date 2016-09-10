from smv import *
from pyspark.sql.functions import col
from pyspark.sql import DataFrame

class InputZipCounty(SmvPyCsvFile):
    def __init__(self, smv):
        super(InputZipCounty, self).__init__(smv, "pdda_raw/00_geo/zip_to_county.csv")

class PyZipPrimaryCounty(SmvPyModule):
    def requiresDS(self):
        return [InputZipCounty]

    def compute(self, i):
        super(PyZipPrimaryCounty, self).compute(i)

        df = i[InputZipCounty]

        filtered = df.select(
            df['ZCTA5'].alias("Zip"),
            df['GEOID'].alias("County"),
            df['POPPT'].alias("Population"),
            df['HUPT'].alias("HouseHold")
        )

        grouped = filtered.smvGroupBy(col("Zip"))
        p1 = grouped.smvTopNRecs(1, col('Population').desc())
        p2 = p1.select(p1["Zip"], p1["County"].alias("PCounty"))
        return filtered.smvJoinByKey(p2, ["Zip"], "inner").repartition(8)
