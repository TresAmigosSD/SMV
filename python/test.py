from smv import *
from pyspark.sql.functions import col
from pyspark.sql import DataFrame

class InputZipCounty(SmvPyCsvFile):
    def path(self):
        return "pdda_raw/00_geo/zip_to_county.csv"

    def csvAttr(self):
        return self.defaultCsvWithHeader()

class PyZipPrimaryCounty(SmvPyModule):
    """For each Zip assign the County with the largets population as the primary county"""

    def requiresDS(self):
        return [InputZipCounty]

    def run(self, i):
        self.prerun(i)

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
