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
import fnmatch
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

class Smv(object):
    """Creates a proxy to SmvApp.

    The SmvApp instance is exposed through the `app` attribute.
    """

    def init(self, arglist, _sc = None, _sqlContext = None):
        sc = SparkContext(appName="smvapp.py") if _sc is None else _sc
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

        # convert python arglist to java String array
        self.create_smv_app(arglist)

        # issue #429 set application name from smv config
        sc._conf.setAppName(self._jsmv.j_smvapp().smvConfig().appName())

        # user may choose a port for the callback server
        gw = sc._gateway
        cbsp = self._jsmv.callbackServerPort()
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
        self.module_file_map = self.get_module_code_file_mapping()
        return self

    def get_module_code_file_mapping(self):
        '''
        This function returns a dictionary where the key is the module name and the
        value is the absolute file path of this module.
        '''
        def get_all_files_with_suffix(path, suffix):
            '''
            This function recurseively searches for all the files with certain
            suffix under certain path and return the absolute file names in a list.
            '''
            matches = []
            for root, dirnames, filenames in os.walk(path):
                for filename in fnmatch.filter(filenames, '*.%s' % suffix):
                    matches.append(os.path.join(root, filename))
            return matches

        def get_module_file_mapping(files, patterns):
            module_dict = {}
            for file in files:
                with open(file, 'rb') as readfile:
                    for line in readfile.readlines():
                        for pattern in patterns:
                            m = re.search(pattern, line)
                            if m:
                                module_name = m.group(1).strip()
                                file_name = file
                                fqn = get_fqn(module_name, file_name)
                                module_dict[fqn] = file_name
            return module_dict

        def get_fqn(module_name, file_name):
            sep = os.path.sep
            file_name_split = file_name.strip().split(sep)
            start_index = file_name_split.index('com')
            if file_name_split[-1].endswith('.scala'):
                file_name_split.pop()
            elif file_name_split[-1].endswith('.py'):
                file_name_split[-1] = file_name_split[-1][:-3]
            fqn_split = file_name_split[start_index:]
            fqn_split.append(module_name)
            fqn = '.'.join(fqn_split)
            return fqn

        code_dir = os.getcwd() + '/src'
        scala_files = get_all_files_with_suffix(code_dir, 'scala')
        python_files = get_all_files_with_suffix(code_dir, 'py')

        files = scala_files + python_files
        patterns = [
            'object (.+?) extends( )+SmvModule\(',
            'object (.+?) extends( )+SmvCsvFile\(',
            'class (.+?)\(SmvPyModule',
            'class (.+?)\(SmvPyCsvFile',
        ]
        module_dict = get_module_file_mapping(files, patterns)
        return module_dict

    def create_smv_app(self, arglist):
        java_args =  smv_copy_array(self.sc, *arglist)
        self._jsmv = self._jvm.org.tresamigos.smv.python.SmvPythonAppFactory.init(java_args, self.sqlContext._ssql_ctx)
        return self._jsmv

    def get_module_code(self, module_name):
        file_name = self.module_file_map[module_name]
        with open(file_name, 'rb') as f:
            lines = f.readlines()
        return lines

    def runModule(self, fqn):
        """Runs either a Scala or a Python SmvModule by its Fully Qualified Name(fqn)
        """
        jdf = self._jsmv.runModule(fqn, self.repo)
        return DataFrame(jdf, self.sqlContext)

    def runDynamicModule(self, fqn):
        """Re-run a Scala or Python module by its fqn"""
        if self.repo.hasDataSet(fqn):
            self.repo.reloadDs(fqn)
        return DataFrame(self._jsmv.runDynamicModule(fqn, self.repo), self.sqlContext)

    def publishModule(self, fqn):
        """Publish a Scala or a Python SmvModule by its FQN
        """
        self._jsmv.publishModule(fqn, self.repo)

    def publishHiveModule(self, fqn):
        """Publish a python SmvModule (by FQN) to a hive table.
           This currently only works with python modules as the repo concept needs to be revisited.
        """
        ds = SmvApp.repo.dsForName(fqn)
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
        self._jsmv.exportDataFrameToHive(jdf, tableName)

    def outputDir(self):
        return self._jsmv.outputDir()

    def scalaOption(self, val):
        """Returns a Scala Option containing the value"""
        return self._jvm.scala.Option.apply(val)

    def run_python_module(self, name):
        klass = for_name(name)
        if (klass in self.pymods):
            return self.pymods[klass]
        else:
            return self.__resolve(klass, [klass])

    def createDF(self, schema, data):
        return DataFrame(self._jsmv.dfFrom(schema, data), self.sqlContext)

    def __resolve(self, klass, stack):
        mod = klass(self)
        for dep in mod.requiresDS():
            if (dep in stack):
                raise RuntimeError("Circular module dependency detected", dep.name(), stack)

            stack.append(dep)
            res = self.__resolve(dep, stack)
            self.pymods[dep] = res
            stack.pop()

        tryRead = self._jsmv.tryReadPersistedFile(mod.modulePath())
        if (tryRead.isSuccess()):
            ret = DataFrame(tryRead.get(), self.sqlContext)
        else:
            _ret = mod.doRun(None, self.pymods)
            if not mod.isEphemeral():
                self._jsmv.persist(_ret._jdf, mod.modulePath(), False)
                ret = DataFrame(self._jsmv.tryReadPersistedFile(mod.modulePath()).get(), self.sqlContext)
            else:
                ret = _ret

        self.pymods[klass] = ret
        return ret

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

    def __init__(self, smvapp):
        self.smvapp = smvapp

    def description(self):
        return self.__doc__

    @abc.abstractmethod
    def requiresDS(self):
        """The list of dataset dependencies"""

    def smvDQM(self):
        """Factory method for Scala SmvDQM"""
        return self.smvapp._jvm.SmvDQM.apply()

    # Factory methods for DQM policies
    def FailParserCountPolicy(self, threshold):
        return self.smvapp._jvm.FailParserCountPolicy(threshold)

    def FailTotalRuleCountPolicy(self, threshold):
        return self.smvapp._jvm.FailTotalRuleCountPolicy(threshold)

    def FailTotalFixCountPolicy(self, threshold):
        return self.smvapp._jvm.FailTotalFixCountPolicy(threshold)

    def FailTotalRulePercentPolicy(self, threshold):
        return self.smvapp._jvm.FailTotalRulePercentPolicy(threshold * 1.0)

    def FailTotalFixPercentPolicy(self, threshold):
        return self.smvapp._jvm.FailTotalFixPercentPolicy(threshold * 1.0)

    # DQM task policies
    def FailNone(self):
        return self.smvapp._jvm.DqmTaskPolicies.failNone()

    def FailAny(self):
        return self.smvapp._jvm.DqmTaskPolicies.failAny()

    def FailCount(self, threshold):
        return self.smvapp._jvm.FailCount(threshold)

    def FailPercent(self, threshold):
        return self.smvapp._jvm.FailPercent(threshold * 1.0)

    def DQMRule(self, rule, name = None, taskPolicy = None):
        task = taskPolicy or self.FailNone()
        return self.smvapp._jvm.DQMRule(rule._jc, name, task)

    def DQMFix(self, condition, fix, name = None, taskPolicy = None):
        task = taskPolicy or self.FailNone()
        return self.smvapp._jvm.DQMFix(condition._jc, fix._jc, name, task)

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
                        res += m(self.smvapp).datasetHash()
                except: pass

        # ensure python's numeric type can fit in a java.lang.Integer
        return res & 0x7fffffff

    def hashOfHash(self):
        res = hash(self.version() + str(self.datasetHash()))

        # include datasetHash of dependency modules
        for m in self.requiresDS():
            res += m(self.smvapp).datasetHash()

        # ensure python's numeric type can fit in a java.lang.Integer
        return res & 0x7fffffff

    def modulePath(self):
        return self.smvapp._jsmv.outputDir() + "/" + self.name() + "_" + hex(self.hashOfHash() & 0xffffffff)[2:] + ".csv"

    @classmethod
    def name(cls):
        """Returns the fully qualified name
        """
        return cls.__module__ + "." + cls.__name__

    def isEphemeral(self):
        """If set to true, the run result of this dataset will not be persisted
        """
        return False

class SmvPyCsvFile(SmvPyDataSet):
    """Raw input file in CSV format
    """

    def __init__(self, smvapp):
        super(SmvPyCsvFile, self).__init__(smvapp)
        self._smvCsvFile = smvapp._jsmv.smvCsvFile(
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

    def __mkCsvAttr(self, delimiter=',', quotechar='""', hasHeader=False):
        """Factory method for creating instances of Scala case class CsvAttributes"""
        return self.smvapp._jvm.org.tresamigos.smv.CsvAttributes(delimiter, quotechar, hasHeader)

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

    def doRun(self, validator, known):
        jdf = self._smvCsvFile.doRun(validator, known)
        return DataFrame(jdf, self.smvapp.sqlContext)

class SmvPyCsvStringData(SmvPyDataSet):
    """Input data from a Schema String and Data String
    """

    def __init__(self, smvapp):
        super(SmvPyCsvStringData, self).__init__(smvapp)
        self._smvCsvStringData = self.smvapp._jvm.org.tresamigos.smv.SmvCsvStringData(
            self.schemaStr(),
            self.dataStr(),
            False
        )

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
        return DataFrame(jdf, self.smvapp.sqlContext)


class SmvPyHiveTable(SmvPyDataSet):
    """Input data source from a Hive table
    """

    def __init__(self, smvapp):
        super(SmvPyHiveTable, self).__init__(smvapp)
        self._smvHiveTable = self.smvapp._jvm.org.tresamigos.smv.SmvHiveTable(self.tableName())

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
        return self.run(DataFrame(self._smvHiveTable.rdd(), self.smvapp.sqlContext))

class SmvPyModule(SmvPyDataSet):
    """Base class for SmvModules written in Python
    """

    IsSmvPyModule = True

    def __init__(self, smvapp):
        super(SmvPyModule, self).__init__(smvapp)

    @abc.abstractmethod
    def run(self, i):
        """This defines the real work done by this module"""

    def doRun(self, validator, known):
        i = {}
        for dep in self.requiresDS():
            # temporarily keep the old code working
            try:
                i[dep] = known[dep]
            except:
                # the values in known are Java DataFrames
                i[dep] = DataFrame(known[dep.name()], self.smvapp.sqlContext)
        return self.run(i)

class SmvPyModuleLink(SmvPyModule):
    """A module link provides access to data generated by modules from another stage
    """

    IsSmvPyModuleLink = True

    def isEphemeral(self):
        return True

    def requiresDS(self):
        return []

    @abc.abstractproperty
    def target(self):
        """Returns the target SmvModule class from another stage to which this link points"""

    def datasetHash(self, includeSuperClass=True):
        stage = self.smvapp._jsmv.inferStageNameFromDsName(self.target().name())
        # TODO get hashOfHash of external target module
        dephash = hash(stage.get()) if stage.isDefined() else self.target()(self.smvapp).datasetHash()
        # ensure python's numeric type can fit in a java.lang.Integer
        return (dephash + super(SmvPyModuleLink, self).datasetHash(includeSuperClass)) & 0x7fffffff

    def run(self, i):
        raise RuntimeError("Cannot run a module link directly")

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

def _is_external(modfqn):
    return modfqn.startswith(ExtDsPrefix)

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

Column.smvStrToTimestamp = lambda c, fmt: Column(colhelper(c).smvStrToTimestamp(fmt))

class PythonDataSetRepository(object):
    def __init__(self, smvapp):
        self.smvapp = smvapp
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
                ret = for_name(modfqn)(self.smvapp)
            except AttributeError: # module not found is anticipated
                return None
            except ImportError as e:
                return None
            except Exception as e: # other errors should be reported, such as syntax error
                traceback.print_exc()
                return None
        self.pythonDataSets[modfqn] = ret
        return ret

    def reloadDs(self, modfqn):
        """Reload the module by its fully qualified name, replace the old
        instance with a new one from the new definnition.
        """
        if self.isExternal(modfqn):
            return self.dsForName(modfqn)

        lastdot = modfqn.rfind('.')
        if (lastdot == -1):
            klass = reload(modfqn)
        else:
            mod = reload(sys.modules[modfqn[:lastdot]])
            klass = getattr(mod, modfqn[lastdot+1:])
        ret = klass(self.smvapp)
        self.pythonDataSets[modfqn] = ret

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

    def hasDataSet(self, modfqn):
        return self.dsForName(modfqn) is not None

    def isExternal(self, modfqn):
        return _is_external(modfqn)

    def getExternalDsName(self, modfqn):
        ds = self.dsForName(modfqn)
        if ds is None:
            self.notFound(modfqn, "cannot get external dataset name")
        elif self.isExternal(modfqn):
            return ds.refname
        else:
            return ''

    def isLink(self, modfqn):
        ds = self.dsForName(modfqn)
        if ds is None:
            self.notFound(modfqn, "cannot test if it is link")
        else:
            try:
                return ds.IsSmvPyModuleLink
            except:
                return False

    def getLinkTargetName(self, modfqn):
        ds = self.dsForName(modfqn)
        if ds is None:
            self.notFound(modfqn, "cannot get link target")
        else:
            return ds.target().name()

    def isEphemeral(self, modfqn):
        if self.isExternal(modfqn):
            raise ValueError("Cannot know if {0} is ephemeral because it is external".format(modfqn))
        else:
            ds = self.dsForName(modfqn)
            if ds is None:
                self.notFound(modfqn, "cannot get isEphemeral")
            else:
                return ds.isEphemeral()

    def getDqm(self, modfqn):
        ds = self.dsForName(modfqn)
        if ds is None:
            self.notFound(modfqn, "cannot get dqm")
        else:
            return ds.dqm()

    def notFound(self, modfqn, msg):
        raise ValueError("dataset [{0}] is not found in {1}: {2}".format(modfqn, self.__class__.__name__, msg))

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
                            buf.append(obj.name())
                    except AttributeError:
                        continue
        return ','.join(buf)

    def dependencies(self, modfqn):
        if self.isExternal(modfqn):
            return ''
        else:
            ds = self.dsForName(modfqn)
            if ds is None:
                self.notFound(modfqn, "cannot get dependencies")
            else:
                return ','.join([x.name() for x in ds.requiresDS()])

    def getDataFrame(self, modfqn, validator, known):
        ds = self.dsForName(modfqn)
        if ds is None:
            self.notFound(modfqn, "cannot get dataframe")
        else:
            try:
                ret = ds.doRun(validator, known)._jdf
            except Exception as e:
                print("----------------------------------------")
                print("Error when running Python SmvModule [{0}]".format(modfqn))
                traceback.print_exc()
                print("----------------------------------------")
                raise e
            return ret

    def rerun(self, modfqn, validator, known):
        ds = self.reloadDs(modfqn)
        if ds is None:
            self.notFound(modfqn, "cannot rerun")
        return ds.doRun(validator, known)._jdf

    def datasetHash(self, modfqn, includeSuperClass):
        ds = self.dsForName(modfqn)
        if ds is None:
            self.notFound(modfqn, "cannot calc dataset hash")
        else:
            return ds.datasetHash(includeSuperClass)

    class Java:
        implements = ['org.tresamigos.smv.SmvDataSetRepository']
