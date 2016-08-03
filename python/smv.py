# provides df.peek() in python shell
def peek(df):
    smv = df._sc._jvm.org.tresamigos.smv.python.SmvPythonProxy()
    return smv.peek(df._jdf)

# provides df.selectPlus(...) in python shell
# example: df.selectPlus(lit(1).alias('col'))
def selectPlus(df, *col):
    smv = df._sc._jvm.org.tresamigos.smv.python.SmvPythonProxy()
    cols = df._sc._gateway.new_array(df._sc._jvm.org.apache.spark.sql.Column, len(col))
    for i in range(0, len(col)):
        cols[i] = col[i]._jc
    return smv.selectPlus(df._jdf, cols)

# enhances the spark DataFrame with smv helper functions
from pyspark.sql import DataFrame
DataFrame.peek = peek
DataFrame.selectPlus = selectPlus

class Smv(object):
    """Creates a proxy to SmvApp.

    The SmvApp instance is exposed through the `app` attribute.
    """

    def __init__(self, sqlContext):
        self.sqlContext = sqlContext
        self._jvm = sqlContext._sc._jvm
        self.smv = self._jvm.org.tresamigos.smv.python.SmvPythonProxy()
        self.app = self.smv.init(sqlContext._ssql_ctx)

    def runModule(self, fqn):
        """Runs a Scala SmvModule by its Fully Qualified Name(fqn)
        """
        jdf = self.app.runModuleByName(fqn)
        return DataFrame(jdf, self.sqlContext)

    def runDynamic(self, fqn):
        """Dynamically runs a Scala SmvModule by its Fully Qualified Name(fqn)
        """
        jdf = self.app.runDynamicModuleByName(fqn)
        return DataFrame(jdf, self.sqlContext)
