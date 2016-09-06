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
        self.pymods = {}

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
        if (name in self.pymods):
            return self.pymods[name]
        else:
            mod = for_name(name)(self)
            return self.__resolve(mod, [name])

    def __resolve(self, mod, stack):
        for dep in mod.requiresDS():
            depname = dep.fqn()
            if (depname in stack):
                raise RuntimeError("Circular module dependency detected", dep, stack)

            stack.append(depname)
            res = self.__resolve(dep, stack)
            self.pymods[depname] = res
            stack.pop()

        # TODO: read from persisted if any
        ret = mod.compute()

        if not mod.isInput():
            self.app.persist(mod.compute()._jdf, mod.modulePath(), True)

        self.pymods[mod.fqn()] = ret
        return ret

class SmvPyDataSet(object):
    """Base class for all SmvDataSets written in Python
    """

    def __init__(self, smv):
        self.smv = smv

    def requiresDS(self):
        return []

    def modulePath(self):
        return self.smv.app.outputDir() + "/" + self.fqn() + ".csv"

    def fqn(self):
        """Returns the fully qualified name
        """
        return self.__module__ + "." + self.__class__.__name__

    def isInput(self):
        return False

class SmvPyCsvFile(SmvPyDataSet):
    """Raw input file in CSV format
    """

    # TODO: add csv attributes
    def __init__(self, smv, path):
        super(SmvPyCsvFile, self).__init__(smv)
        self._smvCsvFile = smv.app.smvCsvFile(path)

    def isInput(self):
        return True

    def compute(self):
        jdf = self._smvCsvFile.rdd()
        return DataFrame(jdf, self.smv.sqlContext)

class SmvPyModule(SmvPyDataSet):
    """Base class for SmvModules written in Python
    """

    def __init__(self, smv):
        super(SmvPyModule, self).__init__(smv)

    def compute(self):
        print(".... computing module " + self.fqn())
