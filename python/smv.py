def for_name(name):
    """Dynamically load a class by its name.

    Equivalent to Java's Class.forName
    """
    components = name.split('.')
    mod = __import__(components[0])
    for comp in components[1:]:
        mod = getattr(mod, comp)
    return mod

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

    def __init__(self, arglist, sqlContext):
        self.sqlContext = sqlContext
        sc = sqlContext._sc
        self._jvm = sc._jvm
        self.proxy = self._jvm.org.tresamigos.smv.python.SmvPythonProxy()

        # convert python arglist to java String array
        java_args = sc._gateway.new_array(sc._jvm.String, len(arglist))
        for i in range(0, len(java_args)):
            java_args[i] = arglist[i]

        factory = self._jvm.org.tresamigos.smv.python.SmvPythonAppFactory()
        self.app = factory.init(java_args, sqlContext._ssql_ctx)

    def runModule(self, fqn):
        """Runs a Scala SmvModule by its Fully Qualified Name(fqn)
        """
        jdf = self.app.app.runModuleByName(fqn)
        return DataFrame(jdf, self.sqlContext)

    def runDynamic(self, fqn):
        """Dynamically runs a Scala SmvModule by its Fully Qualified Name(fqn)
        """
        jdf = self.app.app.runDynamicModuleByName(fqn)
        return DataFrame(jdf, self.sqlContext)

    def run_python_module(self, name):
        mod = for_name(name)()
        # TODO: run dependent modules first
        self.app.persist(mod.compute(self)._jdf, "tmp/result", True)

class SmvPyModule(object):
    """Base class for SmvModules written in Python
    """

    def __init__(self):
        pass

    def compute(self, app):
        print(".... computing module " + self.__module__ + "." + self.__class__.__name__)
