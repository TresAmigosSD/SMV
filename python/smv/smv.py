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

from pyspark import SparkContext
from pyspark.sql import HiveContext, DataFrame
from pyspark.sql.column import Column
from pyspark.sql.functions import col
from utils import smv_copy_array

import abc

import inspect
import sys
import traceback

if sys.version >= '3':
    basestring = unicode = str
    long = int
    from io import StringIO
    from importlib import reload
else:
    from cStringIO import StringIO

# TODO: private
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

# TODO: private
def smvhash(text):
    """Python's hash function will return different numbers from run to
    from, starting from 3.  Provide a deterministic hash function for
    use to calculate datasetHash.
    """
    import binascii
    return binascii.crc32(text)

# TODO: private
def stripComments(code):
    import re
    code = str(code)
    return re.sub(r'(?m)^ *(#.*\n?|[ \t]*\n)', '', code)

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

class SmvPyOutput(object):
    """Marks an SmvPyModule as one of the output of its stage"""
    IsSmvPyOutput = True

    def tableName(self):
        """The table name this SmvPyOutput will export to"""
        return None

class SmvPyDataSet(object):
    """Base class for all SmvDataSets written in Python
    """

    # Python's issubclass() check does not work well with dynamically
    # loaded modules.  In addition, there are some issues with the
    # check, when the `abc` module is used as a metaclass, that we
    # don't yet quite understand.  So for a workaround we add the
    # typcheck in the Smv hierarchies themselves.
    IsSmvPyDataSet = True

    __metaclass__ = abc.ABCMeta

    def __init__(self, smvPy):
        self.smvPy = smvPy

    def description(self):
        return self.__doc__

    @abc.abstractmethod
    def requiresDS(self):
        """The list of dataset dependencies"""

    def smvDQM(self):
        """Factory method for Scala SmvDQM"""
        return self.smvPy._jvm.SmvDQM.apply()

    # Factory methods for DQM policies
    def FailParserCountPolicy(self, threshold):
        return self.smvPy._jvm.FailParserCountPolicy(threshold)

    def FailTotalRuleCountPolicy(self, threshold):
        return self.smvPy._jvm.FailTotalRuleCountPolicy(threshold)

    def FailTotalFixCountPolicy(self, threshold):
        return self.smvPy._jvm.FailTotalFixCountPolicy(threshold)

    def FailTotalRulePercentPolicy(self, threshold):
        return self.smvPy._jvm.FailTotalRulePercentPolicy(threshold * 1.0)

    def FailTotalFixPercentPolicy(self, threshold):
        return self.smvPy._jvm.FailTotalFixPercentPolicy(threshold * 1.0)

    # DQM task policies
    def FailNone(self):
        return self.smvPy._jvm.DqmTaskPolicies.failNone()

    def FailAny(self):
        return self.smvPy._jvm.DqmTaskPolicies.failAny()

    def FailCount(self, threshold):
        return self.smvPy._jvm.FailCount(threshold)

    def FailPercent(self, threshold):
        return self.smvPy._jvm.FailPercent(threshold * 1.0)

    def DQMRule(self, rule, name = None, taskPolicy = None):
        task = taskPolicy or self.FailNone()
        return self.smvPy._jvm.DQMRule(rule._jc, name, task)

    def DQMFix(self, condition, fix, name = None, taskPolicy = None):
        task = taskPolicy or self.FailNone()
        return self.smvPy._jvm.DQMFix(condition._jc, fix._jc, name, task)

    def dqm(self):
        """Subclasses with a non-default DQM policy should override this method"""
        return self.smvDQM()

    @abc.abstractmethod
    def doRun(self, validator, known):
        """Comput this dataset, and return the dataframe"""

    def version(self):
        """All datasets are versioned, with a string,
        so that code and the data it produces can be tracked together."""
        return "0";

    def isOutput(self):
        return isinstance(self, SmvPyOutput)

    def datasetHash(self):
        cls = self.__class__
        try:
            src = inspect.getsource(cls)
            src_no_comm = stripComments(src)
            # DO NOT use the compiled byte code for the hash computation as
            # it doesn't change when constant values are changed.  For example,
            # "a = 5" and "a = 6" compile to same byte code.
            # co_code = compile(src, inspect.getsourcefile(cls), 'exec').co_code
            # TODO: may need to remove comments at the end of line from src code above.
            res = smvhash(src_no_comm)
        except Exception as err: # `inspect` will raise error for classes defined in the REPL
            # Instead of handle the case that module defined in REPL, just raise Exception here
            # res = smvhash(disassemble(cls))
            message = "{0}({1!r})".format(type(err).__name__, err.args)
            raise Exception(message + "\n" + "SmvDataSet defined in shell can't be persisted")

        # include datasetHash of parent classes
        for m in inspect.getmro(cls):
            try:
                if m.IsSmvPyDataSet and m != cls and not m.fqn().startswith("smv."):
                    res += m(self.smvPy).datasetHash()
            except: pass

        # ensure python's numeric type can fit in a java.lang.Integer
        return res & 0x7fffffff

    @classmethod
    def fqn(cls):
        """Returns the fully qualified name
        """
        return cls.__module__ + "." + cls.__name__

    @classmethod
    def urn(cls):
        return "mod:" + cls.fqn()

    def isEphemeral(self):
        """If set to true, the run result of this dataset will not be persisted
        """
        return False

    def getDqm(self):
        return self.dqm()

    def dependencies(self):
        # Try/except block is a short-term solution (read: hack) to ensure that
        # the user gets a full stack trace when SmvPyDataSet user-defined methods
        # causes errors
        try:
            arr = smv_copy_array(self.smvPy.sc, *[x.urn() for x in self.requiresDS()])
        except BaseException as err:
            traceback.print_exc()
            raise err

        return arr

    def getDataFrame(self, validator, known):
        # Try/except block is a short-term solution (read: hack) to ensure that
        # the user gets a full stack trace when SmvPyDataSet user-defined methods
        # causes errors
        try:
            df = self.doRun(validator, known)._jdf
        except BaseException as err:
            traceback.print_exc()
            raise err

        return df

    class Java:
        implements = ['org.tresamigos.smv.ISmvModule']

class SmvPyCsvFile(SmvPyDataSet):
    """Raw input file in CSV format
    """

    def __init__(self, smvPy):
        super(SmvPyCsvFile, self).__init__(smvPy)
        self._smvCsvFile = smvPy.j_smvPyClient.smvCsvFile(
            self.fqn(), self.path(), self.csvAttr(),
            self.forceParserCheck(), self.failAtParsingError())

    def description(self):
        return "Input file: @" + self.path()

    def forceParserCheck(self):
        return True

    def failAtParsingError(self):
        return True

    @abc.abstractproperty
    def path(self):
        """The path to the csv input file"""

    def defaultCsvWithHeader(self):
        return self.smvPy.defaultCsvWithHeader()

    def defaultTsv(self):
        return self.smvPy.defaultTsv()

    def defaultTsvWithHeader(self):
        return self.smvPy.defaultTsvWithHeader()

    def csvAttr(self):
        """Specifies the csv file format.  Corresponds to the CsvAttributes case class in Scala.
        """
        return None

    def isEphemeral(self):
        return True

    def requiresDS(self):
        return []

    def doRun(self, validator, known):
        jdf = self._smvCsvFile.doRun(validator)
        return DataFrame(jdf, self.smvPy.sqlContext)

class SmvPyCsvStringData(SmvPyDataSet):
    """Input data from a Schema String and Data String
    """

    def __init__(self, smvPy):
        super(SmvPyCsvStringData, self).__init__(smvPy)
        self._smvCsvStringData = self.smvPy._jvm.org.tresamigos.smv.SmvCsvStringData(
            self.schemaStr(),
            self.dataStr(),
            False
        )

    def isEphemeral(self):
        return True

    @abc.abstractproperty
    def schemaStr(self):
        """Smv Schema string. E.g. "id:String; dt:Timestamp"
        """

    @abc.abstractproperty
    def dataStr(self):
        """Smv data string. E.g. "212,2016-10-03;119,2015-01-07"
        """

    def requiresDS(self):
        return []

    def doRun(self, validator, known):
        jdf = self._smvCsvStringData.doRun(validator)
        return DataFrame(jdf, self.smvPy.sqlContext)


class SmvPyHiveTable(SmvPyDataSet):
    """Input data source from a Hive table
    """

    def __init__(self, smvPy):
        super(SmvPyHiveTable, self).__init__(smvPy)
        self._smvHiveTable = self.smvPy._jvm.org.tresamigos.smv.SmvHiveTable(self.tableName())

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

    def doRun(self, validator, known):
        return self.run(DataFrame(self._smvHiveTable.rdd(), self.smvPy.sqlContext))

class SmvPyModule(SmvPyDataSet):
    """Base class for SmvModules written in Python
    """

    IsSmvPyModule = True

    # need to simulate map from ds to df where the same object can be keyed
    # by different datasets with the same urn. usecase example
    # class X(SmvPyModule):
    #  def requiresDS(self): return [SmvPyModuleLink("foo")]
    #  def run(self, i): return i[SmvPyModuleLink("foo")]
    class RunParams(object):
        # urn2df should be a map from the urn to the df of the corresponding ds
        def __init__(self, urn2df):
            self.urn2df = urn2df
        # __getitem__ is called by [] operator
        def __getitem__(self, ds):
            return self.urn2df[ds.urn()]

    def __init__(self, smvPy):
        super(SmvPyModule, self).__init__(smvPy)

    @abc.abstractmethod
    def run(self, i):
        """This defines the real work done by this module"""

    def doRun(self, validator, known):
        urn2df = {}
        for dep in self.requiresDS():
            urn2df[dep.urn()] = DataFrame(known[dep.urn()], self.smvPy.sqlContext)
        i = self.RunParams(urn2df)
        return self.run(i)

class SmvPyModuleLinkTemplate(SmvPyModule):
    """A module link provides access to data generated by modules from another stage
    """

    IsSmvPyModuleLink = True

    @classmethod
    def urn(cls):
        return 'link:' + cls.target().fqn()

    def isEphemeral(self):
        return True

    def requiresDS(self):
        return []

    @classmethod
    def target(cls):
        """Returns the target SmvModule class from another stage to which this link points"""
        raise ValueError('Expect to be implemented by subclass')

    def datasetHash(self):
        stage = self.smvPy.j_smvPyClient.inferStageNameFromDsName(self.target().fqn())
        dephash = smvhash(stage.get()) if stage.isDefined() else self.target()(self.smvPy).datasetHash()
        # ensure python's numeric type can fit in a java.lang.Integer
        return (dephash + super(SmvPyModuleLinkTemplate, self).datasetHash()) & 0x7fffffff

    def run(self, i):
        res = self.smvPy.j_smvPyClient.readPublishedData(self.target().fqn())
        return res.get() if res.isDefined() else self.smvPy.runModule(self.target().urn())

PyExtDataSetCache = {}
def SmvPyExtDataSet(refname):
    if refname in PyExtDataSetCache:
        return PyExtDataSetCache[refname]
    cls = type("SmvPyExtDataSet", (SmvPyDataSet,), {
        "refname" : refname,
        "smvPy"   : smvPy,
        "doRun"   : lambda self, validator, known: smvPy.runModule(self.urn)
    })
    cls.fqn = classmethod(lambda klass: refname)
    PyExtDataSetCache[refname] = cls
    return cls

def SmvPyModuleLink(target):
    cls = type("SmvPyModuleLink", (SmvPyModuleLinkTemplate,), {})
    cls.target = classmethod(lambda klass: target)
    return cls

def SmvPyExtModuleLink(refname):
    return SmvPyModuleLink(SmvPyExtDataSet(refname))

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

    def smvPivotCoalesce(self, pivotCols, valueCols, baseOutput):
        """Perform a normal SmvPivot using the pivot and value columns, followed by coalescing all the output.
        pivotCols: list of list of strings
        valueCols: list of strings
        baseOutput: list of strings"""
        return DataFrame(self.sgd.smvPivotCoalesce(smv_copy_array(self.df._sc, *pivotCols), smv_copy_array(self.df._sc, *valueCols), smv_copy_array(self.df._sc, *baseOutput)), self.df.sql_ctx)

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

# Create the SmvPy "Singleton"
from smvpy import SmvPy
smvPy = SmvPy()

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

DataFrame.smvUnpivot = lambda df, *cols: DataFrame(dfhelper(df).smvUnpivot(_to_seq(cols)), df.sql_ctx)

DataFrame.smvExportCsv = lambda df, path, n=None: dfhelper(df).smvExportCsv(path, n)


#############################################
# DfHelpers which print to STDOUT
# Scala side which print to STDOUT will not work on Jupyter. Have to pass the string to python side then print to stdout
#############################################
println = lambda str: sys.stdout.write(str + "\n")

def printFile(f, str):
    tgt = open(f, "w")
    tgt.write(str + "\n")
    tgt.close()

DataFrame.peek = lambda df, pos = 1, colRegex = ".*": println(helper(df).peekStr(df._jdf, pos, colRegex))
DataFrame.peekSave = lambda df, path, pos = 1, colRegex = ".*": printFile(path, helper(df).peekStr(df._jdf, pos, colRegex))

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

def __smvDiscoverPK(df, n):
    res = helper(df).smvDiscoverPK(df._jdf, n)
    println("[{}], {}".format(", ".join(map(str, res._1())), res._2()))

DataFrame.smvDiscoverPK = lambda df, n=10000: __smvDiscoverPK(df, n)

DataFrame.smvDumpDF = lambda df: println(dfhelper(df)._smvDumpDF())

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

Column.smvDay70 = lambda c: Column(colhelper(c).smvDay70())
Column.smvMonth70 = lambda c: Column(colhelper(c).smvMonth70())

Column.smvStrToTimestamp = lambda c, fmt: Column(colhelper(c).smvStrToTimestamp(fmt))
