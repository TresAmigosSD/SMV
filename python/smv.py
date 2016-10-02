from pyspark.sql.column import Column
from pyspark.sql.functions import col
import sys

if sys.version >= '3':
    basestring = unicode = str
    long = int
    from io import StringIO
else:
    from cStringIO import StringIO

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

def disassemble(obj):
    """Disassembles a module and returns bytecode as a string.
    """
    mod = obj if (isinstance(obj, type)) else obj.__class__

    buf = StringIO()
    import dis
    if sys.version >= '3':
        dis.dis(mod, file=buf)
    else:
        stdout = sys.stdout
        sys.stdout = buf
        dis.dis(mod)
        sys.stdout = stdout
    ret = buf.getvalue()
    buf.close()
    return ret

# enhances the spark DataFrame with smv helper functions
from pyspark.sql import DataFrame

# peek from Scala side which print to STDOUT will not work on Jupyter. Have to pass the string to python side then print to stdout
import sys
DataFrame.peek = lambda df, pos = 1, colRegex = ".*": sys.stdout.write(df._sc._jvm.org.tresamigos.smv.python.SmvPythonHelper.peekStr(df._jdf, pos, colRegex) + "\n")

# provides df.selectPlus(...) in python shell
# example: df.selectPlus(lit(1).alias('col'))
# TODO: remove
DataFrame.selectPlus = lambda df, *cols: DataFrame(df._sc._jvm.org.tresamigos.smv.python.SmvPythonHelper.selectPlus(df._jdf, smv_copy_array(df._sc, *cols)), df.sql_ctx)

DataFrame.smvExpandStruct = lambda df, *cols: DataFrame(df._sc._jvm.org.tresamigos.smv.python.SmvPythonHelper.smvExpandStruct(df._jdf, smv_copy_array(df._sc, *cols)), df.sql_ctx)

DataFrame.smvGroupBy = lambda df, *cols: SmvGroupedData(df, df._sc._jvm.org.tresamigos.smv.python.SmvPythonHelper.smvGroupBy(df._jdf, smv_copy_array(df._sc, *cols)))

DataFrame.smvJoinByKey = lambda df, other, keys, joinType: DataFrame(df._sc._jvm.org.tresamigos.smv.python.SmvPythonHelper.smvJoinByKey(df._jdf, other._jdf, smv_copy_array(df._sc, *keys), joinType), df.sql_ctx)

def __smvHashSample(df, key, rate=0.01, seed=23):
    if (isinstance(key, basestring)):
        jkey = col(key)._jc
    elif (isinstance(key, Column)):
        jkey = key._jc
    else:
        raise RuntimeError("key parameter must be either a String or a Column")
    return DataFrame(df._sc._jvm.org.tresamigos.smv.python.SmvPythonHelper.smvHashSample(df._jdf, jkey, rate, seed), df.sql_ctx)
DataFrame.smvHashSample = __smvHashSample

DataFrame.smvSelectMinus = lambda df, *cols: DataFrame(df._sc._jvm.org.tresamigos.smv.python.SmvPythonHelper.smvSelectMinus(df._jdf, smv_copy_array(df._sc, *cols)), df.sql_ctx)

DataFrame.smvSelectPlus = lambda df, *cols: DataFrame(df._sc._jvm.org.tresamigos.smv.python.SmvPythonHelper.smvSelectPlus(df._jdf, smv_copy_array(df._sc, *cols)), df.sql_ctx)

DataFrame.smvDedupByKey = lambda df, keys: DataFrame(df._sc._jvm.org.tresamigos.smv.python.SmvPythonHelper.smvDedupByKey(df._jdf, smv_copy_array(df._sc, *keys)), df.sql_ctx)

import abc
from pyspark import SparkContext
from pyspark.sql import HiveContext

class Smv(object):
    """Creates a proxy to SmvApp.

    The SmvApp instance is exposed through the `app` attribute.
    """

    def init(self, arglist, _sc = None, _sqlContext = None):

        #TODO: appName should be read from the config files
        #      need to process the arglist first and create smvConfig before init SmvApp
        sc = SparkContext(appName="smvapp.py") if _sc is None else _sc
        sqlContext = HiveContext(sc) if _sqlContext is None else _sqlContext

        self.sqlContext = sqlContext
        self._jvm = sc._jvm

        # convert python arglist to java String array
        java_args =  smv_copy_array(sc, *arglist)
        self.app = self._jvm.org.tresamigos.smv.python.SmvPythonAppFactory.init(java_args, sqlContext._ssql_ctx)
        self.pymods = {}
        return self

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

    def createDF(self, schema, data):
        return DataFrame(self.app.dfFrom(schema, data), self.sqlContext)

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

    @classmethod
    def hashsource(cls, src, fname='inline'):
        return hash(compile(src, fname, 'exec'))

    @classmethod
    def datasetHash(cls):
        try:
            import inspect
            src = inspect.getsource(cls)
            res = cls.hashsource(src, inspect.getsourcefile(cls))
        except: # `inspect` will raise error for classes defined in the REPL
            res = hash(disassemble(cls))
        return res

    def hashOfHash(self):
        res = hash(self.version() + str(self.datasetHash()))

        # include datasetHash of dependency modules
        if sys.version >= '3':
            from functools import reduce
        res = reduce(lambda x,y: x+y, [m.datasetHash() for m in self.requiresDS()], res)

        return res

    def modulePath(self):
        return self.smv.app.outputDir() + "/" + self.fqn() + "_" + hex(self.hashOfHash() & 0xffffffff)[2:] + ".csv"

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
        return "Input file: @" + self.path()

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

class SmvPyHiveTable(SmvPyDataSet):
    """Input data source from a Hive table
    """

    def __init__(self, smv):
        super(SmvPyHiveTable, self).__init__(smv)
        self._smvHiveTable = self.smv._jvm.org.tresamigos.smv.SmvHiveTable(self.tableName())

    def description(self):
        return "Hive Table: @" + self.tableName()

    @abc.abstractproperty
    def tableName(self):
        """The qualified Hive table name"""

    def isInput(self):
        return True

    def requiresDS(self):
        return []

    def run(self, i):
        return DataFrame(self._smvHiveTable.rdd(), self.smv.sqlContext)

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

# Create the SmvApp "Singleton"
SmvApp = Smv()

# ColumnHelper methods:
Column.smvStrToTimestamp = lambda c, fmt: Column(SmvApp._jvm.org.tresamigos.smv.ColumnHelper(c._jc).smvStrToTimestamp(fmt))
