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
"""SMV DataSet Framework interface

This module defines the abstract classes which formed the SmvDataSet Framework for clients' projects
"""

from pyspark import SparkContext
from pyspark.sql import HiveContext, DataFrame
from pyspark.sql.column import Column
from pyspark.sql.functions import col
from utils import smv_copy_array

import abc

import inspect
import sys
import traceback

from dqm import SmvDQM
from error import SmvRuntimeError

if sys.version >= '3':
    basestring = unicode = str
    long = int
    from io import StringIO
    from importlib import reload
else:
    from cStringIO import StringIO

def _disassemble(obj):
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

def _smvhash(text):
    """Python's hash function will return different numbers from run to
    from, starting from 3.  Provide a deterministic hash function for
    use to calculate datasetHash.
    """
    import binascii
    return binascii.crc32(text)

def _stripComments(code):
    import re
    code = str(code)
    return re.sub(r'(?m)^ *(#.*\n?|[ \t]*\n)', '', code)

class SmvOutput(object):
    """Marks an SmvModule as one of the output of its stage"""
    IsSmvOutput = True

    def tableName(self):
        """The table name this SmvOutput will export to"""
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

    def dqm(self):
        """Subclasses with a non-default DQM policy should override this method"""
        return SmvDQM()

    @abc.abstractmethod
    def doRun(self, validator, known):
        """Comput this dataset, and return the dataframe"""

    def version(self):
        """All datasets are versioned, with a string,
        so that code and the data it produces can be tracked together."""
        return "0";

    def isOutput(self):
        return isinstance(self, SmvOutput)

    def datasetHash(self):
        cls = self.__class__
        try:
            src = inspect.getsource(cls)
            src_no_comm = _stripComments(src)
            # DO NOT use the compiled byte code for the hash computation as
            # it doesn't change when constant values are changed.  For example,
            # "a = 5" and "a = 6" compile to same byte code.
            # co_code = compile(src, inspect.getsourcefile(cls), 'exec').co_code
            res = _smvhash(src_no_comm)
        except Exception as err: # `inspect` will raise error for classes defined in the REPL
            # Instead of handle the case that module defined in REPL, just raise Exception here
            # res = _smvhash(_disassemble(cls))
            message = "{0}({1!r})".format(type(err).__name__, err.args)
            raise Exception(message + "\n" + "SmvDataSet defined in shell can't be persisted")

        # include datasetHash of parent classes
        for m in inspect.getmro(cls):
            try:
                if m.IsSmvPyDataSet and m != cls and not m.fqn().startswith("smv."):
                    res += m(self.smvPy).datasetHash()
            except: pass

        # if module inherits from SmvRunConfig, then add hash of all config values to module hash
        if hasattr(self, "getRunConfigHash"):
            res += self.getRunConfigHash()

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

    @abc.abstractmethod
    def dsType(self):
        """Return SmvPyDataSet's type"""

    def getDqm(self):
        try:
            res = self.dqm()
        except BaseException as err:
            traceback.print_exc()
            raise err

        return res

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
            df = self.doRun(validator, known)
            if not isinstance(df, DataFrame):
                raise SmvRuntimeError(self.fqn() + " produced " + type(df).__name__ + " in place of a DataFrame")
            else:
                jdf = df._jdf
        except BaseException as err:
            traceback.print_exc()
            raise err

        return jdf

    class Java:
        implements = ['org.tresamigos.smv.ISmvModule']

class SmvPyInput(SmvPyDataSet):
    """Input DataSet, requiredDS is empty and isEphemeral is true"""
    def isEphemeral(self):
        return True

    def dsType(self):
        return "Input"

    def requiresDS(self):
        return []

    def run(self, df):
        """This can be override by concrete SmvPyInput"""
        return df

class WithParser(object):
    """shared parser funcs"""

    def forceParserCheck(self):
        return True

    def failAtParsingError(self):
        return True

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

class SmvCsvFile(SmvPyInput, WithParser):
    """Raw input file in CSV format
    """

    def __init__(self, smvPy):
        super(SmvCsvFile, self).__init__(smvPy)
        self._smvCsvFile = smvPy.j_smvPyClient.smvCsvFile(
            self.fqn(), self.path(), self.csvAttr(),
            self.forceParserCheck(), self.failAtParsingError())

    def description(self):
        return "Input file: @" + self.path()

    @abc.abstractproperty
    def path(self):
        """The path to the csv input file"""

    def doRun(self, validator, known):
        jdf = self._smvCsvFile.doRun(validator)
        df = self.run(DataFrame(jdf, self.smvPy.sqlContext))

class SmvMultiCsvFiles(SmvPyInput, WithParser):
    """Instead of a single input file, specify a data dir with files which has
       the same schema and CsvAttributes.
    """

    def __init__(self, smvPy):
        super(SmvMultiCsvFiles, self).__init__(smvPy)
        self._smvMultiCsvFiles = smvPy._jvm.org.tresamigos.smv.SmvMultiCsvFiles(
            self.dir(),
            self.csvAttr(),
            None
        )

    def description(self):
        return "Input dir: @" + self.dir()

    @abc.abstractproperty
    def dir(self):
        """The path to the csv input dir"""

    def doRun(self, validator, known):
        jdf = self._smvMultiCsvFiles.doRun(validator)
        return self.run(DataFrame(jdf, self.smvPy.sqlContext))

class SmvCsvStringData(SmvPyInput):
    """Input data from a Schema String and Data String
    """

    def __init__(self, smvPy):
        super(SmvCsvStringData, self).__init__(smvPy)
        self._smvCsvStringData = self.smvPy._jvm.org.tresamigos.smv.SmvCsvStringData(
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

    def doRun(self, validator, known):
        jdf = self._smvCsvStringData.doRun(validator)
        return self.run(DataFrame(jdf, self.smvPy.sqlContext))


class SmvHiveTable(SmvPyInput):
    """Input data source from a Hive table
    """

    def __init__(self, smvPy):
        super(SmvHiveTable, self).__init__(smvPy)
        self._smvHiveTable = self.smvPy._jvm.org.tresamigos.smv.SmvHiveTable(self.tableName())

    def description(self):
        return "Hive Table: @" + self.tableName()

    @abc.abstractproperty
    def tableName(self):
        """The qualified Hive table name"""

    def doRun(self, validator, known):
        return self.run(DataFrame(self._smvHiveTable.rdd(), self.smvPy.sqlContext))

class SmvModule(SmvPyDataSet):
    """Base class for SmvModules written in Python
    """

    IsSmvModule = True


    def dsType(self):
        return "Module"

    # need to simulate map from ds to df where the same object can be keyed
    # by different datasets with the same urn. usecase example
    # class X(SmvModule):
    #  def requiresDS(self): return [SmvModuleLink("foo")]
    #  def run(self, i): return i[SmvModuleLink("foo")]
    class RunParams(object):
        # urn2df should be a map from the urn to the df of the corresponding ds
        def __init__(self, urn2df):
            self.urn2df = urn2df
        # __getitem__ is called by [] operator
        def __getitem__(self, ds):
            return self.urn2df[ds.urn()]

    def __init__(self, smvPy):
        super(SmvModule, self).__init__(smvPy)

    @abc.abstractmethod
    def run(self, i):
        """This defines the real work done by this module"""

    def doRun(self, validator, known):
        urn2df = {}
        for dep in self.requiresDS():
            urn2df[dep.urn()] = DataFrame(known[dep.urn()], self.smvPy.sqlContext)
        i = self.RunParams(urn2df)
        return self.run(i)

class SmvModuleLinkTemplate(SmvModule):
    """A module link provides access to data generated by modules from another stage
    """

    IsSmvModuleLink = True

    @classmethod
    def urn(cls):
        return 'link:' + cls.target().fqn()

    def isEphemeral(self):
        return True

    def dsType(self):
        return "Link"

    def requiresDS(self):
        return []

    @classmethod
    def target(cls):
        """Returns the target SmvModule class from another stage to which this link points"""
        raise ValueError('Expect to be implemented by subclass')

    def run(self, i):
        res = self.smvPy.j_smvPyClient.readPublishedData(self.target().fqn())
        return res.get() if res.isDefined() else self.smvPy.runModule(self.target().urn())

PyExtDataSetCache = {}

from smvpy import smvPy

def SmvExtDataSet(refname):
    if refname in PyExtDataSetCache:
        return PyExtDataSetCache[refname]
    cls = type("SmvExtDataSet", (SmvPyDataSet,), {
        "refname" : refname,
        "smvPy"   : smvPy,
        "doRun"   : lambda self, validator, known: smvPy.runModule(self.urn)
    })
    cls.fqn = classmethod(lambda klass: refname)
    PyExtDataSetCache[refname] = cls
    return cls

def SmvModuleLink(target):
    cls = type("SmvModuleLink", (SmvModuleLinkTemplate,), {})
    cls.target = classmethod(lambda klass: target)
    return cls

def SmvExtModuleLink(refname):
    return SmvModuleLink(SmvExtDataSet(refname))

# For backwards compatibility
SmvPyOutput = SmvOutput
SmvPyExtDataSet = SmvExtDataSet
SmvPyCsvFile = SmvCsvFile
SmvPyCsvStringData = SmvCsvStringData
SmvPyMultiCsvFiles = SmvMultiCsvFiles
SmvPyHiveTable = SmvHiveTable
SmvPyModule = SmvModule
SmvPyModuleLink = SmvModuleLink
SmvPyExtModuleLink = SmvExtModuleLink
