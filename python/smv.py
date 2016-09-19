from pyspark.sql.column import Column
import sys

if sys.version >= '3':
    basestring = unicode = str
    long = int

def for_name(name):
    """Dynamically load a class by its name.

    Equivalent to Java's Class.forName
    """
    lastdot = name.rfind('.')
    if (lastdot == -1):
        return getattr(__import('__main__'), name)

    mod = __import__(name[:lastdot])
    for comp in name.split('.')[1:]:
        mod = getattr(mod, comp)
    return mod

def smv_copy_array(sc, *cols):
    """Copy Python list to appropriate Java array
    """
    if (len(cols) == 0):        # may need to pass the correct java type somehow
        return sc._gateway.new_array(sc._jvm.java.lang.String, 0)

    elem = cols[0]
    if (isinstance(elem, basestring)):
        jcols = sc._gateway.new_array(sc._jvm.java.lang.String, len(cols))
        for i in range(0, len(jcols)):
            jcols[i] = cols[i]
    elif (isinstance(elem, Column)):
        jcols = sc._gateway.new_array(sc._jvm.org.apache.spark.sql.Column, len(cols))
        for i in range(0, len(jcols)):
            jcols[i] = cols[i]._jc
    elif (isinstance(elem, list)): # a list of list
        # use Java List as the outermost container; an Array[Array]
        # will not always work, because the inner list may be of
        # different lengths
        jcols = sc._jvm.java.util.ArrayList()
        for i in range(0, len(cols)):
            jcols.append(smv_copy_array(sc, *cols[i]))
    else:
        raise RuntimeError("Cannot copy array of type", type(elem))

    return jcols

# enhances the spark DataFrame with smv helper functions
from pyspark.sql import DataFrame
DataFrame.peek = lambda df: df._sc._jvm.org.tresamigos.smv.python.SmvPythonHelper.peek(df._jdf)

# provides df.selectPlus(...) in python shell
# example: df.selectPlus(lit(1).alias('col'))
DataFrame.selectPlus = lambda df, *cols: DataFrame(df._sc._jvm.org.tresamigos.smv.python.SmvPythonHelper.selectPlus(df._jdf, smv_copy_array(df._sc, *cols)), df.sql_ctx)

DataFrame.smvGroupBy = lambda df, *cols: SmvGroupedData(df, df._sc._jvm.org.tresamigos.smv.python.SmvPythonHelper.smvGroupBy(df._jdf, smv_copy_array(df._sc, *cols)))

DataFrame.smvJoinByKey = lambda df, other, keys, joinType: DataFrame(df._sc._jvm.org.tresamigos.smv.python.SmvPythonHelper.smvJoinByKey(df._jdf, other._jdf, smv_copy_array(df._sc, *keys), joinType), df.sql_ctx)

import abc

class Smv(object):
    """Creates a proxy to SmvApp.

    The SmvApp instance is exposed through the `app` attribute.
    """

    def __init__(self, arglist, sqlContext):
        self.sqlContext = sqlContext
        sc = sqlContext._sc
        self._jvm = sc._jvm

        # convert python arglist to java String array
        java_args = sc._gateway.new_array(sc._jvm.String, len(arglist))
        for i in range(0, len(java_args)):
            java_args[i] = arglist[i]

        self.app = self._jvm.org.tresamigos.smv.python.SmvPythonAppFactory.init(java_args, sqlContext._ssql_ctx)
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
        klass = for_name(name)
        if (klass in self.pymods):
            return self.pymods[klass]
        else:
            return self.__resolve(klass, [klass])

    def __resolve(self, klass, stack):
        mod = klass(self)
        for dep in mod.requiresDS():
            if (dep in stack):
                raise RuntimeError("Circular module dependency detected", dep.fqn(), stack)

            stack.append(dep)
            res = self.__resolve(dep, stack)
            self.pymods[dep] = res
            stack.pop()

        tryRead = self.app.tryReadPersistedFile(mod.modulePath())
        if (tryRead.isSuccess()):
            ret = DataFrame(tryRead.get(), self.sqlContext)
        else:
            ret = mod.run(self.pymods)
            if not mod.isInput():
                self.app.persist(ret._jdf, mod.modulePath(), True)

        self.pymods[mod] = ret
        return ret

class SmvPyDataSet(object):
    """Base class for all SmvDataSets written in Python
    """

    __metaclass__ = abc.ABCMeta

    def __init__(self, smv):
        self.smv = smv

    @abc.abstractmethod
    def description(self):
        """A brief description of this dataset"""

    @abc.abstractmethod
    def requiresDS(self):
        """The list of dataset dependencies"""

    @abc.abstractmethod
    def run(self, i):
        """Comput this dataset, including its depencies if necessary"""

    def version(self):
        """All datasets are versioned, with a string,
        so that code and the data it produces can be tracked together."""
        return "0";

    def modulePath(self):
        return self.smv.app.outputDir() + "/" + self.fqn() + "_" + self.version() + ".csv"

    def fqn(self):
        """Returns the fully qualified name
        """
        return self.__module__ + "." + self.__class__.__name__

    def isInput(self):
        return False

class SmvPyCsvFile(SmvPyDataSet):
    """Raw input file in CSV format
    """

    def __init__(self, smv):
        super(SmvPyCsvFile, self).__init__(smv)
        self._smvCsvFile = smv.app.smvCsvFile(self.fqn() + "_" + self.version(), self.path(), self.csvAttr())

    def description(self):
        return "Input file @" + self.path()

    @abc.abstractproperty
    def path(self):
        """The path to the csv input file"""

    def __mkCsvAttr(self, delimiter=',', quotechar='""', hasHeader=False):
        """Factory method for creating instances of Scala case class CsvAttributes"""
        return self.smv._jvm.org.tresamigos.smv.CsvAttributes(delimiter, quotechar, hasHeader)

    def defaultCsvWithHeader(self):
        return self.__mkCsvAttr(hasHeader=True)

    def defaultTsv(self):
        return self.__mkCsvAttr(delimiter='\t')

    def defaultTsvWitHeader(self):
        return self.__mkCsvAttr(delimier='\t', hasHeader=True)

    def csvAttr(self):
        """Specifies the csv file format.  Corresponds to the CsvAttributes case class in Scala.
        """
        return None

    def isInput(self):
        return True

    def requiresDS(self):
        return []

    def run(self, i):
        jdf = self._smvCsvFile.rdd()
        return DataFrame(jdf, self.smv.sqlContext)

class SmvPyModule(SmvPyDataSet):
    """Base class for SmvModules written in Python
    """

    def __init__(self, smv):
        super(SmvPyModule, self).__init__(smv)

    def prerun(self, i):
        print(".... computing module " + self.fqn())

class SmvGroupedData(object):
    """Wrapper around the Scala SmvGroupedData"""
    def __init__(self, df, sgd):
        self.df = df
        self.sgd = sgd

    def smvTopNRecs(self, maxElems, *cols):
        return DataFrame(self.sgd.smvTopNRecs(maxElems, smv_copy_array(self.df._sc, *cols)), self.df.sql_ctx)

    def smvPivotSum(self, pivotCols, valueCols, baseOutput):
        """Perform a normal SmvPivot using the pivot and value columns, followed by summing all the output.
        TODO: docstring parameters and return values
        pivotCols: list of list of strings
        valueCols: list of strings
        baseOutput: list of strings"""
        return DataFrame(self.sgd.smvPivotSum(smv_copy_array(self.df._sc, *pivotCols), smv_copy_array(self.df._sc, *valueCols), smv_copy_array(self.df._sc, *baseOutput)), self.df.sql_ctx)
