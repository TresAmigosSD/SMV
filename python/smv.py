from pyspark.sql.column import Column
from pyspark.sql.functions import col

import inspect
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
        return getattr(__import__('__main__'), name)

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
    elif (isinstance(elem, DataFrame)):
        jcols = sc._gateway.new_array(sc._jvm.org.apache.spark.sql.DataFrame, len(cols))
        for i in range(0, len(jcols)):
            jcols[i] = cols[i]._jdf
    elif (isinstance(elem, list)): # a list of list
        # use Java List as the outermost container; an Array[Array]
        # will not always work, because the inner list may be of
        # different lengths
        jcols = sc._jvm.java.util.ArrayList()
        for i in range(0, len(cols)):
            jcols.append(smv_copy_array(sc, *cols[i]))
    elif (isinstance(elem, tuple)):
        jcols = sc._jvm.java.util.ArrayList()
        for i in range(0, len(cols)):
            # Use Java List for tuple
            je = sc._jvm.java.util.ArrayList()
            for e in cols[i]: je.append(e)
            jcols.append(je)
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

    def helper(self):
        return self._jvm.org.tresamigos.smv.python.SmvPythonHelper

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

    def description(self):
        return self.__doc__

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
            src = inspect.getsource(cls)
            res = cls.hashsource(src, inspect.getsourcefile(cls))
        except: # `inspect` will raise error for classes defined in the REPL
            res = hash(disassemble(cls))

        # include datasetHash of parent classes
        for m in inspect.getmro(cls):
            if (issubclass(m, SmvPyDataSet) and m != SmvPyDataSet and m != cls and m != object):
                res += m.datasetHash()

        return res

    def hashOfHash(self):
        res = hash(self.version() + str(self.datasetHash()))

        # include datasetHash of dependency modules
        for m in self.requiresDS():
            res += m.datasetHash()

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

class SmvMultiJoin(object):
    """Wrapper around Scala's SmvMultiJoin"""
    def __init__(self, sqlContext, mj):
        self.sqlContext = sqlContext
        self.mj = mj

    def joinWith(self, df, postfix, jointype = 'inner'):
        return SmvMultiJoin(self.sqlContext, self.mj.joinWith(df._jdf, postfix, jointype))

    def doJoin(self, dropextra = False):
        return DataFrame(self.mj.doJoin(dropextra), self.sqlContext)

# Create the SmvApp "Singleton"
SmvApp = Smv()

import sys

helper = lambda df: df._sc._jvm.org.tresamigos.smv.python.SmvPythonHelper

DataFrame.smvExpandStruct = lambda df, *cols: DataFrame(helper(df).smvExpandStruct(df._jdf, smv_copy_array(df._sc, *cols)), df.sql_ctx)

DataFrame.smvGroupBy = lambda df, *cols: SmvGroupedData(df, helper(df).smvGroupBy(df._jdf, smv_copy_array(df._sc, *cols)))

def __smvHashSample(df, key, rate=0.01, seed=23):
    if (isinstance(key, basestring)):
        jkey = col(key)._jc
    elif (isinstance(key, Column)):
        jkey = key._jc
    else:
        raise RuntimeError("key parameter must be either a String or a Column")
    return DataFrame(helper(df).smvHashSample(df._jdf, jkey, rate, seed), df.sql_ctx)
DataFrame.smvHashSample = __smvHashSample

DataFrame.smvJoinByKey = lambda df, other, keys, joinType: DataFrame(helper(df).smvJoinByKey(df._jdf, other._jdf, smv_copy_array(df._sc, *keys), joinType), df.sql_ctx)

DataFrame.smvJoinMultipleByKey = lambda df, keys, joinType = 'inner': SmvMultiJoin(df.sql_ctx, helper(df).smvJoinMultipleByKey(df._jdf, smv_copy_array(df._sc, *keys), joinType))

DataFrame.smvSelectMinus = lambda df, *cols: DataFrame(helper(df).smvSelectMinus(df._jdf, smv_copy_array(df._sc, *cols)), df.sql_ctx)

DataFrame.smvSelectPlus = lambda df, *cols: DataFrame(helper(df).smvSelectPlus(df._jdf, smv_copy_array(df._sc, *cols)), df.sql_ctx)

DataFrame.smvDedupByKey = lambda df, *keys: DataFrame(helper(df).smvDedupByKey(df._jdf, smv_copy_array(df._sc, *keys)), df.sql_ctx)

DataFrame.smvDedupByKeyWithOrder = lambda df, keys, orderCol: DataFrame(helper(df).smvDedupByKeyWithOrder(df._jdf, smv_copy_array(df._sc, *keys), smv_copy_array(df._sc, *orderCol)), df.sql_ctx)

DataFrame.smvUnion = lambda df, *dfothers: DataFrame(helper(df).smvUnion(df._jdf, smv_copy_array(df._sc, *dfothers)), df.sql_ctx)

DataFrame.smvRenameField = lambda df, *namePairs: DataFrame(helper(df).smvRenameField(df._jdf, smv_copy_array(df._sc, *namePairs)), df.sql_ctx)

#############################################
# DfHelpers which print to STDOUT
# Scala side which print to STDOUT will not work on Jupyter. Have to pass the string to python side then print to stdout
#############################################
println = lambda str: sys.stdout.write(str + "\n")

DataFrame.peek = lambda df, pos = 1, colRegex = ".*": println(helper(df).peekStr(df._jdf, pos, colRegex))

def _smvEdd(df, *cols): return helper(df).smvEdd(df._jdf, smv_copy_array(df._sc, *cols))
def _smvHist(df, *cols): return helper(df).smvHist(df._jdf, smv_copy_array(df._sc, *cols))
def _smvConcatHist(df, cols): return helper(df).smvConcatHist(df._jdf, smv_copy_array(df._sc, *cols))
def _smvFreqHist(df, *cols): return helper(df).smvFreqHist(df._jdf, smv_copy_array(df._sc, *cols))
def _smvCountHist(df, keys, binSize): return helper(df).smvCountHist(df._jdf, smv_copy_array(df._sc, *keys), binSize)
def _smvBinHist(df, *colWithBin):
    for elem in colWithBin:
        assert type(elem) is tuple, "smvBinHist takes a list of tuple(string, double) as paraeter"
        assert len(elem) == 2, "smvBinHist takes a list of tuple(string, double) as parameter"
    insureDouble = map(lambda t: (t[0], t[1] * 1.0), colWithBin)
    return helper(df).smvBinHist(df._jdf, smv_copy_array(df._sc, *insureDouble))

DataFrame.smvEdd = lambda df, *cols: println(_smvEdd(df, *cols))
DataFrame.smvHist = lambda df, *cols: println(_smvHist(df, *cols))
DataFrame.smvConcatHist = lambda df, cols: println(_smvConcatHist(df, cols))
DataFrame.smvFreqHist = lambda df, *cols: println(_smvFreqHist(df, *cols))

def __smvCountHistFn(df, keys, binSize = 1):
    if (isinstance(keys, basestring)):
        return println(_smvCountHist(df, [keys], binSize))
    else:
        return println(_smvCountHist(df, keys, binSize))
DataFrame.smvCountHist = __smvCountHistFn

DataFrame.smvBinHist = lambda df, *colWithBin: println(_smvBinHist(df, *colWithBin))

#############################################
# ColumnHelper methods:
#############################################
Column.smvStrToTimestamp = lambda c, fmt: Column(SmvApp._jvm.org.tresamigos.smv.ColumnHelper(c._jc).smvStrToTimestamp(fmt))
