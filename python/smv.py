#
# This file is licensed under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from py4j.java_gateway import java_import, JavaObject

from pyspark import SparkContext
from pyspark.sql import HiveContext, DataFrame
from pyspark.sql.column import Column
from pyspark.sql.functions import col

import abc

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

# common converters to pass to _to_seq and _to_list
def _jcol(c): return c._jc
def _jdf(df): return df._jdf

# Modified from Spark column.py
def _to_seq(cols, converter=None):
    """
    Convert a list of Column (or names) into a JVM Seq of Column.

    An optional `converter` could be used to convert items in `cols`
    into JVM Column objects.
    """
    if converter:
        cols = [converter(c) for c in cols]
    return _sparkContext()._jvm.PythonUtils.toSeq(cols)

# Modified from Spark column.py
def _to_list(cols, converter=None):
    """
    Convert a list of Column (or names) into a JVM (Scala) List of Column.

    An optional `converter` could be used to convert items in `cols`
    into JVM Column objects.
    """
    if converter:
        cols = [converter(c) for c in cols]
    return _sparkContext()._jvm.PythonUtils.toList(cols)

def _sparkContext():
    return SparkContext._active_spark_context

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

        from py4j.java_gateway import java_import
        java_import(self._jvm, "org.tresamigos.smv.ColumnHelper")
        java_import(self._jvm, "org.tresamigos.smv.SmvDFHelper")
        java_import(self._jvm, "org.tresamigos.smv.python.SmvPythonHelper")

        # convert python arglist to java String array
        java_args =  smv_copy_array(sc, *arglist)
        self.app = self._jvm.org.tresamigos.smv.python.SmvPythonAppFactory.init(java_args, sqlContext._ssql_ctx)

        # user may choose a port for the callback server
        gw = sc._gateway
        cbsp = self.app.callbackServerPort()
        cbs_port = cbsp.get() if cbsp.isDefined() else gw._python_proxy_port

        # this was a workaround for py4j 0.8.2.1, shipped with spark
        # 1.5.x, to prevent the callback server from hanging the
        # python, and hence the java, process
        from pyspark.streaming.context import _daemonize_callback_server
        _daemonize_callback_server()

        if "_callback_server" not in gw.__dict__ or gw._callback_server is None:
            print("starting callback server on port {0}".format(cbs_port))
            gw._shutdown_callback_server() # in case another has already started
            gw._start_callback_server(cbs_port)
            gw._python_proxy_port = gw._callback_server.port
            # get the GatewayServer object in JVM by ID
            jgws = JavaObject("GATEWAY_SERVER", gw._gateway_client)
            # update the port of CallbackClient with real port
            gw.jvm.SmvPythonHelper.updatePythonGatewayPort(jgws, gw._python_proxy_port)

        self.pymods = {}
        self.repo = PythonDataSetRepository(self)
        return self

    def runModule(self, fqn):
        """Runs either a Scala or a Python SmvModule by its Fully Qualified Name(fqn)
        """
        jdf = self.app.runModule(fqn, self.repo)
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
                raise RuntimeError("Circular module dependency detected", dep.name(), stack)

            stack.append(dep)
            res = self.__resolve(dep, stack)
            self.pymods[dep] = res
            stack.pop()

        tryRead = self.app.tryReadPersistedFile(mod.modulePath())
        if (tryRead.isSuccess()):
            ret = DataFrame(tryRead.get(), self.sqlContext)
        else:
            _ret = mod.doRun(mod.dqm(), self.pymods)
            if not mod.isEphemeral():
                self.app.persist(_ret._jdf, mod.modulePath(), False)
                ret = DataFrame(self.app.tryReadPersistedFile(mod.modulePath()).get(), self.sqlContext)
            else:
                ret = _ret

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

    def dqm(self):
        """DQM Factory method"""
        return self.smv._jvm.org.tresamigos.smv.dqm.SmvDQM.apply()

    def createDsDqm(self):
        """Subclasses with a non-default DQM policy should override this method"""
        return self.dqm()

    @abc.abstractmethod
    def doRun(self, dsDqm, known):
        """Comput this dataset, and return the dataframe"""

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

        # ensure python's long type can fit in a java.lang.Long
        return res & 0x7fffffffffffffff

    def hashOfHash(self):
        res = hash(self.version() + str(self.datasetHash()))

        # include datasetHash of dependency modules
        for m in self.requiresDS():
            res += m.datasetHash()

        return res

    def modulePath(self):
        return self.smv.app.outputDir() + "/" + self.name() + "_" + hex(self.hashOfHash() & 0xffffffff)[2:] + ".csv"

    @classmethod
    def name(cls):
        """Returns the fully qualified name
        """
        return cls.__module__ + "." + cls.__name__

    def isEphemeral(self):
        """Does this dataset need to be persisted?
        """
        return False

class SmvPyCsvFile(SmvPyDataSet):
    """Raw input file in CSV format
    """

    def __init__(self, smv):
        super(SmvPyCsvFile, self).__init__(smv)
        self._smvCsvFile = smv.app.smvCsvFile(self.name() + "_" + self.version(), self.path(), self.csvAttr())

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

    def isEphemeral(self):
        return True

    def requiresDS(self):
        return []

    def doRun(self, dsDqm, known):
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

    def isEphemeral(self):
        return True

    def requiresDS(self):
        return []

    def run(self, df):
        """This can be override by concrete SmvPyHiveTable"""
        return df

    def doRun(self, dsDqm, known):
        return self.run(DataFrame(self._smvHiveTable.rdd(), self.smv.sqlContext))

class SmvPyModule(SmvPyDataSet):
    """Base class for SmvModules written in Python
    """

    def __init__(self, smv):
        super(SmvPyModule, self).__init__(smv)

    def prerun(self, i):
        print(".... computing module " + self.name())

    @abc.abstractmethod
    def run(self, i):
        """This defines the real work done by this module"""

    def doRun(self, dsDqm, known):
        i = {}
        for dep in self.requiresDS():
            # temporarily keep the old code working
            try:
                i[dep] = known[dep]
            except:
                # the values in known are Java DataFrames
                i[dep] = DataFrame(known[dep.name()], self.smv.sqlContext)
        return self.run(i)

ExtDsPrefix = "SmvPyExtDataSet."
PyExtDataSetCache = {}
def SmvPyExtDataSet(refname):
    if refname in PyExtDataSetCache:
        return PyExtDataSetCache[refname]
    cls = type("SmvPyExtDataSet", (SmvPyDataSet,), {
        "refname" : refname
    })
    cls.name = classmethod(lambda klass: ExtDsPrefix + refname)
    PyExtDataSetCache[refname] = cls
    return cls

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

    def smvFillNullWithPrevValue(self, *orderCols):
        """Fill in Null values with "previous" value according to an ordering
           * Example:
           * Input:
           * K, T, V
           * a, 1, null
           * a, 2, a
           * a, 3, b
           * a, 4, null
           *
           * df.smvGroupBy("K").smvFillNullWithPrevValue($"T".asc)("V")
           *
           * Output:
           * K, T, V
           * a, 1, null
           * a, 2, a
           * a, 3, b
           * a, 4, b
           *
        """
        def __doFill(*valueCols):
            return DataFrame(self.sgd.smvFillNullWithPrevValue(smv_copy_array(self.df._sc, *orderCols), smv_copy_array(self.df._sc, *valueCols)), self.df.sql_ctx)
        return __doFill

class SmvMultiJoin(object):
    """Wrapper around Scala's SmvMultiJoin"""
    def __init__(self, sqlContext, mj):
        self.sqlContext = sqlContext
        self.mj = mj

    def joinWith(self, df, postfix, jointype = None):
        return SmvMultiJoin(self.sqlContext, self.mj.joinWith(df._jdf, postfix, jointype))

    def doJoin(self, dropextra = False):
        return DataFrame(self.mj.doJoin(dropextra), self.sqlContext)

# Create the SmvApp "Singleton"
SmvApp = Smv()

helper = lambda df: df._sc._jvm.SmvPythonHelper
def dfhelper(df):
    return _sparkContext()._jvm.SmvDFHelper(df._jdf)

def colhelper(c):
    return _sparkContext()._jvm.ColumnHelper(c._jc)

DataFrame.smvExpandStruct = lambda df, *cols: DataFrame(helper(df).smvExpandStruct(df._jdf, smv_copy_array(df._sc, *cols)), df.sql_ctx)

# TODO can we port this without going through the proxy?
DataFrame.smvGroupBy = lambda df, *cols: SmvGroupedData(df, helper(df).smvGroupBy(df._jdf, smv_copy_array(df._sc, *cols)))

def __smvHashSample(df, key, rate=0.01, seed=23):
    if (isinstance(key, basestring)):
        jkey = col(key)._jc
    elif (isinstance(key, Column)):
        jkey = key._jc
    else:
        raise RuntimeError("key parameter must be either a String or a Column")
    return DataFrame(dfhelper(df).smvHashSample(jkey, rate, seed), df.sql_ctx)
DataFrame.smvHashSample = __smvHashSample

# FIXME py4j method resolution with null argument can fail, so we
# temporarily remove the trailing parameters till we can find a
# workaround
DataFrame.smvJoinByKey = lambda df, other, keys, joinType: DataFrame(helper(df).smvJoinByKey(df._jdf, other._jdf, _to_seq(keys), joinType), df.sql_ctx)

DataFrame.smvJoinMultipleByKey = lambda df, keys, joinType = 'inner': SmvMultiJoin(df.sql_ctx, helper(df).smvJoinMultipleByKey(df._jdf, smv_copy_array(df._sc, *keys), joinType))

DataFrame.smvSelectMinus = lambda df, *cols: DataFrame(helper(df).smvSelectMinus(df._jdf, smv_copy_array(df._sc, *cols)), df.sql_ctx)

DataFrame.smvSelectPlus = lambda df, *cols: DataFrame(dfhelper(df).smvSelectPlus(_to_seq(cols, _jcol)), df.sql_ctx)

DataFrame.smvDedupByKey = lambda df, *keys: DataFrame(helper(df).smvDedupByKey(df._jdf, smv_copy_array(df._sc, *keys)), df.sql_ctx)

def __smvDedupByKeyWithOrder(df, *keys):
    def _withOrder(*orderCols):
        return DataFrame(helper(df).smvDedupByKeyWithOrder(df._jdf, smv_copy_array(df._sc, *keys), smv_copy_array(df._sc, *orderCols)), df.sql_ctx)
    return _withOrder
DataFrame.smvDedupByKeyWithOrder = __smvDedupByKeyWithOrder

DataFrame.smvUnion = lambda df, *dfothers: DataFrame(dfhelper(df).smvUnion(_to_seq(dfothers, _jdf)), df.sql_ctx)

DataFrame.smvRenameField = lambda df, *namePairs: DataFrame(helper(df).smvRenameField(df._jdf, smv_copy_array(df._sc, *namePairs)), df.sql_ctx)

DataFrame.smvExportCsv = lambda df, path, n=None: dfhelper(df).smvExportCsv(path, n)


#############################################
# DfHelpers which print to STDOUT
# Scala side which print to STDOUT will not work on Jupyter. Have to pass the string to python side then print to stdout
#############################################
println = lambda str: sys.stdout.write(str + "\n")

DataFrame.peek = lambda df, pos = 1, colRegex = ".*": println(helper(df).peekStr(df._jdf, pos, colRegex))

def _smvEdd(df, *cols): return dfhelper(df)._smvEdd(_to_seq(cols))
def _smvHist(df, *cols): return dfhelper(df)._smvHist(_to_seq(cols))
def _smvConcatHist(df, cols): return helper(df).smvConcatHist(df._jdf, smv_copy_array(df._sc, *cols))
def _smvFreqHist(df, *cols): return dfhelper(df)._smvFreqHist(_to_seq(cols))
def _smvCountHist(df, keys, binSize): return dfhelper(df)._smvCountHist(_to_seq(keys), binSize)
def _smvBinHist(df, *colWithBin):
    for elem in colWithBin:
        assert type(elem) is tuple, "smvBinHist takes a list of tuple(string, double) as paraeter"
        assert len(elem) == 2, "smvBinHist takes a list of tuple(string, double) as parameter"
    insureDouble = map(lambda t: (t[0], t[1] * 1.0), colWithBin)
    return helper(df).smvBinHist(df._jdf, smv_copy_array(df._sc, *insureDouble))

def _smvEddCompare(df, df2, ignoreColName): return dfhelper(df)._smvEddCompare(df2._jdf, ignoreColName)

DataFrame.smvEdd = lambda df, *cols: println(_smvEdd(df, *cols))
DataFrame.smvHist = lambda df, *cols: println(_smvHist(df, *cols))
DataFrame.smvConcatHist = lambda df, cols: println(_smvConcatHist(df, cols))
DataFrame.smvFreqHist = lambda df, *cols: println(_smvFreqHist(df, *cols))
DataFrame.smvEddCompare = lambda df, df2, ignoreColName=False: println(_smvEddCompare(df, df2, ignoreColName))

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

# SmvPythonHelper is necessary as frontend to generic Scala functions
Column.smvIsAllIn = lambda c, *vals: Column(_sparkContext()._jvm.SmvPythonHelper.smvIsAllIn(c._jc, _to_seq(vals)))
Column.smvIsAnyIn = lambda c, *vals: Column(_sparkContext()._jvm.SmvPythonHelper.smvIsAnyIn(c._jc, _to_seq(vals)))

Column.smvMonth      = lambda c: Column(colhelper(c).smvMonth())
Column.smvYear       = lambda c: Column(colhelper(c).smvYear())
Column.smvQuarter    = lambda c: Column(colhelper(c).smvQuarter())
Column.smvDayOfMonth = lambda c: Column(colhelper(c).smvDayOfMonth())
Column.smvDayOfWeek  = lambda c: Column(colhelper(c).smvDayOfWeek())
Column.smvHour       = lambda c: Column(colhelper(c).smvHour())

Column.smvPlusDays   = lambda c, delta: Column(colhelper(c).smvPlusDays(delta))
Column.smvPlusWeeks  = lambda c, delta: Column(colhelper(c).smvPlusWeeks(delta))
Column.smvPlusMonths = lambda c, delta: Column(colhelper(c).smvPlusMonths(delta))
Column.smvPlusYears  = lambda c, delta: Column(colhelper(c).smvPlusYears(delta))

Column.smvStrToTimestamp = lambda c, fmt: Column(colhelper(c).smvStrToTimestamp(fmt))

class PythonDataSetRepository(object):
    def __init__(self, smv):
        self.smv = smv
        self.pythonDataSets = {} # SmvPyDataSet FQN -> instance

    def dsForName(self, modfqn):
        """Returns the instance of SmvPyDataSet by its fully qualified name.
        Returns None if the FQN is not a valid SmvPyDataSet name.
        """
        if modfqn in self.pythonDataSets:
            return self.pythonDataSets[modfqn]
        elif self.isExternal(modfqn):
            ret = SmvPyExtDataSet(modfqn[len(ExtDsPrefix):])
        else:
            try:
                ret = for_name(modfqn)(self.smv)
            except:
                return None
        self.pythonDataSets[modfqn] = ret
        return ret

    def hasDataSet(self, modfqn):
        return self.dsForName(modfqn) is not None

    def isExternal(self, modfqn):
        return modfqn.startswith(ExtDsPrefix)

    def isEphemeral(self, modfqn):
        if self.isExternal(modfqn):
            raise ValueError("Cannot know if {0} is ephemeral because it is external".format(modfqn))
        else:
            ds = self.dsForName(modfqn)
            if ds is None:
                self.notFound(modfqn, "cannot get isEphemeral")
            else:
                return ds.isEphemeral()

    def getExternalDsName(self, modfqn):
        ds = self.dsForName(modfqn)
        if ds is None:
            self.notFound(modfqn, "cannot get external dataset name")
        elif self.isExternal(modfqn):
            return ds.refname
        else:
            return ''

    def getDqm(self, modfqn):
        ds = self.dsForName(modfqn)
        if ds is None:
            self.notFound(modfqn, "cannot get dqm")
        else:
            return ds.createDsDqm()

    def notFound(self, modfqn, msg):
        raise ValueError("dataset [{0}] is not found in {1}: {2}".format(modfqn, self.__class__.__name__, msg))

    def dependencies(self, modfqn):
        if (self.isExternal(modfqn)):
            return ''
        else:
            ds = self.dsForName(modfqn)
            if ds is None:
                self.notFound(modfqn, "cannot get dependencies")
            else:
                return ','.join([x.name() for x in ds.requiresDS()])

    def getDataFrame(self, modfqn, validator, modules):
        ds = self.dsForName(modfqn)
        if ds is None:
            self.notFound(modfqn, "cannot get dataframe")
        else:
            return ds.doRun(validator, modules)._jdf

    def datasetHash(self, modfqn, includeSuperClass):
        ds = self.dsForName(modfqn)
        if ds is None:
            self.notFound(modfqn, "cannot calc dataset hash")
        else:
            return ds.datasetHash()

    class Java:
        implements = ['org.tresamigos.smv.SmvDataSetRepository']
