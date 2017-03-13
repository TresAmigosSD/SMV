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
        return isinstance(self, SmvPyOutput)

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
            df = self.doRun(validator, known)._jdf
        except BaseException as err:
            traceback.print_exc()
            raise err

        return df

    class Java:
        implements = ['org.tresamigos.smv.ISmvModule']

class SmvPyInput(SmvPyDataSet):
    """Input DataSet, requiredDS is empty and isEphemeral is true"""
    def isEphemeral(self):
        return True

    def requiresDS(self):
        return []

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

class SmvPyCsvFile(SmvPyInput, WithParser):
    """Raw input file in CSV format
    """

    def __init__(self, smvPy):
        super(SmvPyCsvFile, self).__init__(smvPy)
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
        return DataFrame(jdf, self.smvPy.sqlContext)

class SmvPyMultiCsvFiles(SmvPyInput, WithParser):
    """Instead of a single input file, specify a data dir with files which has
       the same schema and CsvAttributes.
    """

    def __init__(self, smvPy):
        super(SmvPyMultiCsvFiles, self).__init__(smvPy)
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
        #TODO: need to review whether _smvhash(stage.get()) will be good enough
        dephash = _smvhash(stage.get()) if stage.isDefined() else self.target()(self.smvPy).datasetHash()
        # ensure python's numeric type can fit in a java.lang.Integer
        return (dephash + super(SmvPyModuleLinkTemplate, self).datasetHash()) & 0x7fffffff

    def run(self, i):
        res = self.smvPy.j_smvPyClient.readPublishedData(self.target().fqn())
        return res.get() if res.isDefined() else self.smvPy.runModule(self.target().urn())

PyExtDataSetCache = {}

from smvpy import smvPy

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
