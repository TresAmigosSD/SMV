from smv import *

class PyZipPrimaryCounty(SmvPyModule):
    def compute(self, smv):
        super(PyZipPrimaryCounty, self).compute(smv)
        df = smv.sqlContext.createDataFrame([(1, 'Adam'), (2, 'Eve')], ["id", "name"])
        df.printSchema()
        df.show()
        return df
