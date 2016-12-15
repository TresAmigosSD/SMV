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
import pkgutil
import os
import re
import sys
import traceback

if sys.version >= '3':
    basestring = unicode = str
    long = int
    from io import StringIO
    from importlib import reload
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

class SmvPy(object):
    """The Python representation of SMV.

    Its singleton instance is created later in the containing module
    and is named `smvPy`

    Adds `java_imports` to the namespace in the JVM gateway in
    SparkContext (in pyspark).  It also creates an instance of
    SmvPyClient.

    """

    def init(self, arglist, _sc = None, _sqlContext = None):
        sc = SparkContext() if _sc is None else _sc
        sqlContext = HiveContext(sc) if _sqlContext is None else _sqlContext

        sc.setLogLevel("ERROR")

        self.sqlContext = sqlContext
        self.sc = sc
        self._jvm = sc._jvm

        from py4j.java_gateway import java_import
        java_import(self._jvm, "org.tresamigos.smv.ColumnHelper")
        java_import(self._jvm, "org.tresamigos.smv.SmvDFHelper")
        java_import(self._jvm, "org.tresamigos.smv.dqm.*")
        java_import(self._jvm, "org.tresamigos.smv.python.SmvPythonHelper")

        self.j_smvPyClient = self.create_smv_pyclient(arglist)

        # shortcut is meant for internal use only
        self.j_smvApp = self.j_smvPyClient.j_smvApp()

        # issue #429 set application name from smv config
        sc._conf.setAppName(self.appName())

        # user may choose a port for the callback server
        gw = sc._gateway
        cbsp = self.j_smvPyClient.callbackServerPort()
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

        self.repo = PythonDataSetRepository(self)
        self.j_smvPyClient.register('Python', self.repo)
        return self

    def appName(self):
        return self.j_smvApp.smvConfig().appName()

    def create_smv_pyclient(self, arglist):
        '''
        return a smvPyClient instance
        '''
        # convert python arglist to java String array
        java_args =  smv_copy_array(self.sc, *arglist)
        return self._jvm.org.tresamigos.smv.python.SmvPyClientFactory.init(java_args, self.sqlContext._ssql_ctx)

    def get_graph_json(self):
        self.j_smvApp.generateAllGraphJSON()
        file_name = self.appName() + '.json'
        file_path = os.path.sep.join([os.getcwd(), file_name])
        with open(file_path, 'rb') as f:
            lines = f.read()
        return lines

    def runModule(self, fqn):
        """Runs either a Scala or a Python SmvModule by its Fully Qualified Name(fqn)
        """
        jdf = self.j_smvPyClient.runModule(fqn)
        return DataFrame(jdf, self.sqlContext)

    def runDynamicModule(self, fqn):
        """Re-run a Scala or Python module by its fqn"""
        if self.repo.hasDataSet(fqn):
            self.repo.reloadDs(fqn)
        return DataFrame(self.j_smvPyClient.runDynamicModule(fqn), self.sqlContext)

    def urn2fqn(self, urnOrFqn):
        """Extracts the SMV module FQN portion from its URN; if it's already an FQN return it unchanged"""
        return self.j_smvPyClient.urn2fqn(urnOrFqn)

    def publishModule(self, fqn):
        """Publish a Scala or a Python SmvModule by its FQN
        """
        self.j_smvPyClient.publishModule(fqn)

    def publishHiveModule(self, fqn):
        """Publish a python SmvModule (by FQN) to a hive table.
           This currently only works with python modules as the repo concept needs to be revisited.
        """
        ds = smvPy.repo.dsForName(fqn)
        if ds == None:
            raise ValueError("Can not load python module {0} to publish".format(fqn))
        tableName = None
        isOutputModule = None
        try:
            tableName = ds.tableName()
            isOutputModule = ds.IsSmvPyOutput
        except: pass
        if not tableName or not isOutputModule:
            raise ValueError("module {0} must be an python output module and define a tablename to be exported to hive".format(fqn))
        jdf = self.runModule(fqn)._jdf
        self.j_smvPyClient.exportDataFrameToHive(jdf, tableName)

    def outputDir(self):
        return self.j_smvPyClient.outputDir()

    def scalaOption(self, val):
        """Returns a Scala Option containing the value"""
        return self._jvm.scala.Option.apply(val)

    def createDF(self, schema, data):
        return DataFrame(self.j_smvPyClient.dfFrom(schema, data), self.sqlContext)

    def _mkCsvAttr(self, delimiter=',', quotechar='""', hasHeader=False):
        """Factory method for creating instances of Scala case class CsvAttributes"""
        return self._jvm.org.tresamigos.smv.CsvAttributes(delimiter, quotechar, hasHeader)

    def defaultCsvWithHeader(self):
        return self._mkCsvAttr(hasHeader=True)

    def defaultTsv(self):
        return self._mkCsvAttr(delimiter='\t')

    def defaultTsvWithHeader(self):
        return self._mkCsvAttr(delimier='\t', hasHeader=True)

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

    def datasetHash(self, includeSuperClass=True):
        cls = self.__class__
        try:
            src = inspect.getsource(cls)
            res = hash(compile(src, inspect.getsourcefile(cls), 'exec'))
        except: # `inspect` will raise error for classes defined in the REPL
            res = hash(disassemble(cls))

        # include datasetHash of parent classes
        if includeSuperClass:
            for m in inspect.getmro(cls):
                try:
                    if m.IsSmvPyDataSet and m != cls and not m.name().startswith("smv."):
                        res += m(self.smvPy).datasetHash()
                except: pass

        # ensure python's numeric type can fit in a java.lang.Integer
        return res & 0x7fffffff

    @classmethod
    def name(cls):
        """Returns the fully qualified name
        """
        return cls.__module__ + "." + cls.__name__

    @classmethod
    def urn(cls):
        return 'urn:smv:mod:' + cls.name()

    def isEphemeral(self):
        """If set to true, the run result of this dataset will not be persisted
        """
        return False

class SmvPyCsvFile(SmvPyDataSet):
    """Raw input file in CSV format
    """

    def __init__(self, smvPy):
        super(SmvPyCsvFile, self).__init__(smvPy)
        self._smvCsvFile = smvPy.j_smvPyClient.smvCsvFile(
            self.name() + "_" + self.version(), self.path(), self.csvAttr(),
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
        jdf = self._smvCsvFile.doRun(validator, known)
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
        jdf = self._smvCsvStringData.doRun(validator, known)
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

    def __init__(self, smvPy):
        super(SmvPyModule, self).__init__(smvPy)

    @abc.abstractmethod
    def run(self, i):
        """This defines the real work done by this module"""

    def doRun(self, validator, known):
        i = {}
        for dep in self.requiresDS():
            i[dep] = DataFrame(known[dep.urn()], self.smvPy.sqlContext)
        return self.run(i)

class SmvPyModuleLink(SmvPyModule):
    """A module link provides access to data generated by modules from another stage
    """

    IsSmvPyModuleLink = True

    @classmethod
    def urn(cls):
        return 'urn:smv:link:' + cls.name()

    def isEphemeral(self):
        return True

    def requiresDS(self):
        return []

    @abc.abstractproperty
    def target(self):
        """Returns the target SmvModule class from another stage to which this link points"""

    def datasetHash(self, includeSuperClass=True):
        stage = self.smvPy.j_smvPyClient.inferStageNameFromDsName(self.target().name())
        dephash = hash(stage.get()) if stage.isDefined() else self.target()(self.smvPy).datasetHash()
        # ensure python's numeric type can fit in a java.lang.Integer
        return (dephash + super(SmvPyModuleLink, self).datasetHash(includeSuperClass)) & 0x7fffffff

    def run(self, i):
        res = self.smvPy.j_smvPyClient.readPublishedData(self.target().name())
        return res.get() if res.isDefined() else self.smvPy.runModule(self.target().urn())

ExtDsPrefix = "urn:smv:ext:"
PyExtDataSetCache = {}
def SmvPyExtDataSet(refname):
    if refname in PyExtDataSetCache:
        return PyExtDataSetCache[refname]
    cls = type("SmvPyExtDataSet", (SmvPyDataSet,), {
        "refname" : refname,
        "smvPy"   : smvPy,
        "isEphemeral": lambda self: true,
        "requiresDS": lambda self: \
            [smvPy.repo.dsForName(i) if i.startswith(ExtDsPrefix) \
             else SmvPyExtDataSet(i) for i in \
             smvPy.j_smvApp.dependencies(refname).mkString(",").split(',')],
        "doRun"   : lambda self, validator, known: smvPy.runModule(refname)
    })
    cls.name = classmethod(lambda klass: ExtDsPrefix + refname)
    cls.urn = classmethod(lambda klass:  ExtDsPrefix + refname)
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


class PythonDataSetRepository(object):
    def __init__(self, smvPy):
        self.smvPy = smvPy
        self.pythonDataSets = {} # SmvPyDataSet FQN -> instance

    def dsForName(self, modUrn):
        """Returns the instance of SmvPyDataSet by its fully qualified name.
        Returns None if the FQN is not a valid SmvPyDataSet name.
        """
        if modUrn in self.pythonDataSets:
            return self.pythonDataSets[modUrn]
        elif modUrn.startswith(ExtDsPrefix):
            ret = SmvPyExtDataSet(modUrn[len(ExtDsPrefix):])
        else:
            fqn = self.smvPy.urn2fqn(modUrn)
            try:
                ret = for_name(fqn)(self.smvPy)
            except AttributeError: # module not found is anticipated
                return None
            except ImportError as e:
                return None
            except Exception as e: # other errors should be reported, such as syntax error
                traceback.print_exc()
                return None
        self.pythonDataSets[modUrn] = ret
        return ret

    def reloadDs(self, modUrn):
        """Reload the module by its fully qualified name, replace the old
        instance with a new one from the new definnition.
        """
        if modUrn.startswith(ExtDsPrefix):
            return self.dsForName(modUrn)

        lastdot = modUrn.rfind('.')
        if (lastdot == -1):
            klass = reload(modUrn)
        else:
            mod = reload(sys.modules[modUrn[:lastdot]])
            klass = getattr(mod, modUrn[lastdot+1:])
        ret = klass(self.smvPy)
        self.pythonDataSets[modUrn] = ret

        # Python issue https://bugs.python.org/issue1218234
        # need to invalidate inspect.linecache to make dataset hash work
        srcfile = inspect.getsourcefile(ret.__class__)
        if srcfile:
            inspect.linecache.checkcache(srcfile)

        # issue #417
        # recursively reload all dependent modules
        for dep in ret.requiresDS():
            self.reloadDs(dep.name())

        return ret

    def hasDataSet(self, modUrn):
        return self.dsForName(modUrn) is not None

    def isEphemeral(self, modUrn):
        ds = self.dsForName(modUrn)
        if ds is None:
            self.notFound(modUrn, "cannot get isEphemeral")
        else:
            return ds.isEphemeral()

    def getDqm(self, modUrn):
        ds = self.dsForName(modUrn)
        if ds is None:
            self.notFound(modUrn, "cannot get dqm")
        else:
            return ds.dqm()

    def notFound(self, modUrn, msg):
        raise ValueError("dataset [{0}] is not found in {1}: {2}".format(modUrn, self.__class__.__name__, msg))

    def outputModsForStage(self, stageName):
        # `walk_packages` can generate AttributeError if the system has
        # Gtk modules, which are not designed to use with reflection or
        # introspection. Best action to take in this situation is probably
        # to simply suppress the error.
        def err(name): pass
        # print("Error importing module %s" % name)
        # t, v, tb = sys.exc_info()
        # print("type is {0}, value is {1}".format(t, v))
        buf = []
        for loader, name, is_pkg in pkgutil.walk_packages(onerror=err):
            if name.startswith(stageName) and not is_pkg:
                try:
                    pymod = __import__(name)
                except:
                    continue
                else:
                    for c in name.split('.')[1:]:
                        pymod = getattr(pymod, c)

                for n in dir(pymod):
                    obj = getattr(pymod, n)
                    try:
                        if (obj.IsSmvPyOutput and obj.IsSmvPyModule):
                            buf.append(obj.urn())
                    except AttributeError:
                        continue
        return ','.join(buf)

    def dependencies(self, modUrn):
        ds = self.dsForName(modUrn)
        if ds is None:
            self.notFound(modUrn, "cannot get dependencies")
        else:
            return ','.join([x.urn() for x in ds.requiresDS()])

    def getDataFrame(self, modUrn, validator, known):
        ds = self.dsForName(modUrn)
        if ds is None:
            self.notFound(modUrn, "cannot get dataframe")
        else:
            try:
                ret = ds.doRun(validator, known)._jdf
            except Exception as e:
                print("----------------------------------------")
                print("Error when running Python SmvModule [{0}]".format(modUrn))
                traceback.print_exc()
                print("----------------------------------------")
                raise e
            return ret

    def rerun(self, modUrn, validator, known):
        ds = self.reloadDs(modUrn)
        if ds is None:
            self.notFound(modUrn, "cannot rerun")
        return ds.doRun(validator, known)._jdf

    def datasetHash(self, modUrn, includeSuperClass):
        ds = self.dsForName(modUrn)
        if ds is None:
            self.notFound(modUrn, "cannot calc dataset hash")
        else:
            return ds.datasetHash(includeSuperClass)

    class Java:
        implements = ['org.tresamigos.smv.SmvDataSetRepository']
