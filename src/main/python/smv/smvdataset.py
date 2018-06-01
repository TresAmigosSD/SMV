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

from pyspark.sql import DataFrame

import abc
import inspect
import sys
import traceback
import binascii
import json

from smv.dqm import SmvDQM
from smv.error import SmvRuntimeError
from smv.utils import smv_copy_array, pickle_lib, is_string
from smv.py4j_interface import create_py4j_interface_method

if sys.version_info >= (3, 4):
    ABC = abc.ABC
else:
    ABC = abc.ABCMeta('ABC', (), {})


def _smvhash(text):
    """Python's hash function will return different numbers from run to
    from, starting from 3.  Provide a deterministic hash function for
    use to calculate sourceCodeHash.
    """
    import binascii
    return binascii.crc32(text.encode())


def _stripComments(code):
    import re
    code = str(code)
    return re.sub(r'(?m)^ *(#.*\n?|[ \t]*\n)', '', code)

def _sourceHash(module):
    src = inspect.getsource(module)
    src_no_comm = _stripComments(src)
    # DO NOT use the compiled byte code for the hash computation as
    # it doesn't change when constant values are changed.  For example,
    # "a = 5" and "a = 6" compile to same byte code.
    # co_code = compile(src, inspect.getsourcefile(cls), 'exec').co_code
    return _smvhash(src_no_comm)

class SmvOutput(object):
    """Mixin which marks an SmvModule as one of the output of its stage

        SmvOutputs are distinct from other SmvDataSets in that
            * SmvModuleLinks can *only* link to SmvOutputs
            * The -s and --run-app options of smv-run only run SmvOutputs and their dependencies.
    """
    IsSmvOutput = True

    def tableName(self):
        """The user-specified table name used when exporting data to Hive (optional)

            Returns:
                (string)
        """
        return None

    getTableName = create_py4j_interface_method("getTableName", "tableName")


class SmvDataSet(ABC):
    """Abstract base class for all SmvDataSets
    """

    # Python's issubclass() check does not work well with dynamically
    # loaded modules.  In addition, there are some issues with the
    # check, when the `abc` module is used as a metaclass, that we
    # don't yet quite understand.  So for a workaround we add the
    # typcheck in the Smv hierarchies themselves.
    IsSmvDataSet = True

    def __init__(self, smvApp):
        self.smvApp = smvApp

    def smvGetRunConfig(self, key):
        """return the current user run configuration value for the given key."""
        return self.smvApp.getConf(key)
    
    def smvGetRunConfigAsInt(self, key):
        runConfig = self.smvGetRunConfig(key);
        if runConfig is None:
            return None
        return int(runConfig)

    def smvGetRunConfigAsBool(self, key):
        runConfig = self.smvGetRunConfig(key);
        if runConfig is None:
            return None
        sval = runConfig.strip().lower()
        return (sval == "1" or sval == "true")

    def config_hash(self):
        """Integer value representing the SMV config's contribution to the dataset hash

            Only the keys declared in requiresConfig will be considered.
        """
        kvs = [(k, self.smvGetRunConfig(k)) for k in self.requiresConfig()]
        # the config_hash should change IFF the config changes
        # sort keys to ensure config hash is independent from key order
        sorted_kvs = sorted(kvs)
        # we need a unique string representation of sorted_kvs to hash
        # repr should change iff sorted_kvs changes
        kv_str = repr(sorted_kvs)
        return _smvhash(kv_str)

    def description(self):
        return self.__doc__

    getDescription = create_py4j_interface_method("getDescription", "description")

    @abc.abstractmethod
    def requiresDS(self):
        """User-specified list of dependencies

            Override this method to specify the SmvDataSets needed as inputs.

            Returns:
                (list(SmvDataSet)): a list of dependencies
        """
        pass

    def requiresConfig(self):
        """User-specified list of config keys this module depends on

            The given keys and their values will influence the dataset hash
        """
        return []
    
    def requiresLib(self):
        """User-specified list of 'library' dependencies. These are code, other than
            the DataSet's run method that impact its output or behaviour.

            Override this method to assist in re-running this module based on changes
            in other python objects (functions, classes, packages).

            Limitations: For python modules and packages, the 'requiresLib()' method is
            limited to registering changes on the main file of the package (for module
            'foo', that's 'foo.py', for package 'bar', that's 'bar/__init__.py'). This
            means that if a module or package imports other modules, the imported
            module's changes will not impact DataSet hashes.

            Returns:
                (list(module)): a list of library dependencies
        """
        return []

    def dqm(self):
        """DQM policy

            Override this method to define your own DQM policy (optional).
            Default is an empty policy.

            Returns:
                (SmvDQM): a DQM policy
        """
        return SmvDQM()

    @abc.abstractmethod
    def doRun(self, validator, known):
        """Compute this dataset, and return the dataframe"""

    getDoRun = create_py4j_interface_method("getDoRun", "doRun")

    def assert_result_is_dataframe(self, result):
        if not isinstance(result, DataFrame):
            raise SmvRuntimeError(
                self.fqn() + " produced " +
                type(result).__name__ + " in place of a DataFrame"
            )

    def version(self):
        """Version number

            Each SmvDataSet is versioned with a numeric string, so it and its result
            can be tracked together.

            Returns:
                (str): version number of this SmvDataSet
        """
        return "0"

    def isOutput(self):
        return isinstance(self, SmvOutput)

    getIsOutput = create_py4j_interface_method("getIsOutput", "isOutput")

    # Note that the Scala SmvDataSet will combine sourceCodeHash and instanceValHash
    # to compute datasetHash
    def sourceCodeHash(self):
        """Hash computed based on the source code of the dataset's class
        """
        cls = self.__class__
        # get hash of module's source code text
        try:
            res = _sourceHash(cls)
        except Exception as err:  # `inspect` will raise error for classes defined in the REPL
            # Instead of handle the case that module defined in REPL, just raise Exception here
            # res = _smvhash(_disassemble(cls))
            traceback.print_exc()
            message = "{0}({1!r})".format(type(err).__name__, err.args)
            raise Exception(
                message + "\n" + "SmvDataSet " +
                self.urn() + " defined in shell can't be persisted"
            )


        # incorporate source code hash of module's parent classes
        for m in inspect.getmro(cls):
            try:
                # TODO: it probably shouldn't matter if the upstream class is an SmvDataSet - it could be a mixin
                # whose behavior matters but which doesn't inherit from SmvDataSet
                if m.IsSmvDataSet and m != cls and not m.fqn().startswith("smv."):
                    res += m(self.smvApp).sourceCodeHash()
            except: 
                pass

        # NOTE: Until SmvRunConfig (now deprecated) is removed entirely, we consider 2 source code hashes, 
        # config_hash and _smvGetRunConfigHash. The former is influenced by KVs for all keys listed in requiresConfig
        # while latter is influenced by KVs for all keys listed in smv.config.keys.
        # TODO: Is the config really a component of the "source code"? This method is called `sourceCodeHash`, after all.

        # incorporate hash of KVs for config keys listed in requiresConfig
        res += self.config_hash()

        # iterate through libs/modules that this DataSet depends on and use their source towards hash as well
        for lib in self.requiresLib():
            lib_src_hash = _sourceHash(lib)
            res += lib_src_hash

        # if module inherits from SmvRunConfig, then add hash of all config values to module hash
        try:
            res += self._smvGetRunConfigHash()
        except: 
            pass

        # if module has high order historical validation rules, add their hash to sum.
        # they key() of a validator should change if its parameters change.
        if hasattr(cls, "_smvHistoricalValidatorsList"):
            keys_hash = [_smvhash(v._key()) for v in cls._smvHistoricalValidatorsList]
            res += sum(keys_hash)

        # ensure python's numeric type can fit in a java.lang.Integer
        return res & 0x7fffffff

    getSourceCodeHash = create_py4j_interface_method("getSourceCodeHash", "sourceCodeHash")

    def instanceValHash(self):
        """Hash computed based on instance values of the dataset, such as the timestamp of an input file
        """
        return 0

    getInstanceValHash = create_py4j_interface_method("getInstanceValHash", "instanceValHash")

    @classmethod
    def fqn(cls):
        """Returns the fully qualified name
        """
        return cls.__module__ + "." + cls.__name__

    getFqn = create_py4j_interface_method("getFqn", "fqn")

    @classmethod
    def urn(cls):
        return "mod:" + cls.fqn()

    def isEphemeral(self):
        """Should this SmvDataSet skip persisting its data?

            Returns:
                (bool): True if this SmvDataSet should not persist its data, false otherwise
        """
        return False

    getIsEphemeral = create_py4j_interface_method("getIsEphemeral", "isEphemeral")

    def publishHiveSql(self):
        """An optional sql query to run to publish the results of this module when the
           --publish-hive command line is used.  The DataFrame result of running this
           module will be available to the query as the "dftable" table.

            Example:
                >>> return "insert overwrite table mytable select * from dftable"

            Note:
                If this method is not specified, the default is to just create the
                table specified by tableName() with the results of the module.

           Returns:
               (string): the query to run.
        """
        return None

    getPublishHiveSql = create_py4j_interface_method("getPublishHiveSql", "publishHiveSql")

    @abc.abstractmethod
    def dsType(self):
        """Return SmvDataSet's type"""

    getDsType = create_py4j_interface_method("getDsType", "dsType")

    def dqmWithTypeSpecificPolicy(self):
        return self.dqm()

    getDqmWithTypeSpecificPolicy = create_py4j_interface_method(
        "getDqmWithTypeSpecificPolicy", "dqmWithTypeSpecificPolicy"
    )

    def dependencies(self):
        """Can be overridden when a module has non-SmvDataSet dependencies (see SmvModelExec)
        """
        return self.requiresDS()

    def dependencyUrns(self):
        arr = [x.urn() for x in self.dependencies()]
        return smv_copy_array(self.smvApp.sc, *arr)

    getDependencyUrns = create_py4j_interface_method("getDependencyUrns", "dependencyUrns")

    @classmethod
    def df2result(cls, df):
        """Given a datasets's persisted DataFrame, get the result object

            In most cases, this is just the DataFrame itself. See SmvResultModule for the exception.
        """
        return df

    def metadata(self, df):
        """User-defined metadata

            Override this method to define metadata that will be logged with your module's results.
            Defaults to empty dictionary.

            Arguments:
                df (DataFrame): result of running the module, used to generate metadata

            Returns:
                (dict): dictionary of serializable metadata
        """
        return {}

    def metadataJson(self, jdf):
        """Get user's metadata and jsonify it for py4j transport
        """
        df = DataFrame(jdf, self.smvApp.sqlContext)
        metadata = self.metadata(df)
        if not isinstance(metadata, dict):
            raise SmvRuntimeError("User metadata {} is not a dict".format(repr(metadata)))
        return json.dumps(metadata)

    getMetadataJson = create_py4j_interface_method("getMetadataJson", "metadataJson")

    def validateMetadata(self, current, history):
        """User-defined metadata validation

            Override this method to define validation rules for metadata given
            the current metadata and historical metadata.

            Arguments:
                current (dict): current metadata kv
                history (list(dict)): list of historical metadata kv's

            Returns:
                (str): Validation failure message. Return None (or omit a return statement) if
                successful.
        """
        return None

    def validateMetadataJson(self, currentJson, historyJson):
        """Load metadata (jsonified for py4j transport) and run user's validation on it
        """
        current = json.loads(currentJson)
        history = [json.loads(j) for j in historyJson]
        res = self.validateMetadata(current, history)
        if res is not None and not is_string(res):
            raise SmvRuntimeError("Validation failure message {} is not a string".format(repr(res)))
        return res

    getValidateMetadataJson = create_py4j_interface_method("getValidateMetadataJson", "validateMetadataJson")

    def metadataHistorySize(self):
        """Override to define the maximum size of the metadata history for this module

            Return:
                (int): size
        """
        return 5

    getMetadataHistorySize = create_py4j_interface_method("getMetadataHistorySize", "metadataHistorySize")

    class Java:
        implements = ['org.tresamigos.smv.ISmvModule']


class SmvModule(SmvDataSet):
    """Base class for SmvModules written in Python
    """

    IsSmvModule = True


    def dsType(self):
        return "Module"


    class RunParams(object):
        """Map from SmvDataSet to resulting DataFrame

            We need to simulate a dict from ds to df where the same object can be
            keyed by different datasets with the same urn. For example, in the
            module

            class X(SmvModule):
                def requiresDS(self): return [SmvModuleLink("foo")]
                def run(self, i): return i[SmvModuleLink("foo")]

            the i argument of the run method should map SmvModuleLink("foo") to
            the correct DataFrame.

            Args:
                (dict): a map from urn to DataFrame
        """

        def __init__(self, urn2df):
            self.urn2df = urn2df

        def __getitem__(self, ds):
            """Called by the '[]' operator
            """
            if not hasattr(ds, 'urn'):
                raise TypeError('Argument to RunParams must be an SmvDataSet')
            else:
                return self.urn2df[ds.urn()]

    def __init__(self, smvApp):
        super(SmvModule, self).__init__(smvApp)

    @abc.abstractmethod
    def run(self, i):
        """User-specified definition of the operations of this SmvModule

            Override this method to define the output of this module, given a map
            'i' from inputSmvDataSet to resulting DataFrame. 'i' will have a
            mapping for each SmvDataSet listed in requiresDS. E.g.

            def requiresDS(self):
                return [MyDependency]

            def run(self, i):
                return i[MyDependency].select("importantColumn")

            Args:
                (RunParams): mapping from input SmvDataSet to DataFrame

            Returns:
                (DataFrame): ouput of this SmvModule
        """

    def _constructRunParams(self, urn2df):
        """Given dict from urn to DataFrame, construct RunParams for module

            A given module's result may not actually be a DataFrame. For each
            dependency, apply its df2result method to its DataFrame to get its
            actual result. Construct RunParams from the resulting dict.
        """
        urn2res = {}
        for dep in self.dependencies():
            jdf = urn2df[dep.urn()]
            df = DataFrame(jdf, self.smvApp.sqlContext)
            urn2res[dep.urn()] = dep.df2result(df)
        i = self.RunParams(urn2res)
        return i

    def doRun(self, validator, known):
        i = self._constructRunParams(known)
        result = self.run(i)
        self.assert_result_is_dataframe(result)
        return result._jdf


class SmvSqlModule(SmvModule):
    """An SMV module which executes a SQL query in place of a run method
    """
    # User must specify table names. We can't use FQN because the name can't
    # can't contain '.', and defaulting to the module's base name would invite
    # name collisions.
    @abc.abstractmethod
    def tables(self):
        """Dict of dependencies by table name.
        """

    def requiresDS(self):
        return list(self.tables().values())

    @abc.abstractmethod
    def query(self):
        """User-specified SQL query defining the behavior of this module

            Before the query is executed, all dependencies will be registered as
            tables with the names specified in the tables method.
        """

    def run(self, i):
        tbl_name_2_ds = self.tables()

        # temporarily register DataFrame inputs as tables
        for tbl_name in tbl_name_2_ds:
            ds = tbl_name_2_ds[tbl_name]
            i[ds].registerTempTable(tbl_name)

        res = self.smvApp.sqlContext.sql(self.query())

        # drop temporary tables
        for tbl_name in tbl_name_2_ds:
            # This currently causes an "error" to be reported saying "table does
            # not exist". This happens even when using "drop table if exists ".
            # It is annoying but can be safely ignored.
            self.smvApp.sqlContext.sql("drop table " + tbl_name)

        return res


class SmvResultModule(SmvModule):
    """An SmvModule whose result is not a DataFrame

        The result must be picklable - see
        https://docs.python.org/2/library/pickle.html#what-can-be-pickled-and-unpickled.
    """
    @classmethod
    def df2result(self, df):
        """Unpickle and decode module result stored in DataFrame
        """
        # reverses result of applying result2df. see result2df for explanation.
        hex_encoded_pickle_as_str = df.collect()[0][0]
        pickled_res_as_str = binascii.unhexlify(hex_encoded_pickle_as_str)
        res = pickle_lib.loads(pickled_res_as_str)
        return res

    @classmethod
    def result2df(cls, smvApp, res_obj):
        """Pick and encode module result, and store it in a DataFrame
        """
        # pickle the result object. this will use the most optimal pickling
        # protocol available for this version of cPickle
        pickled_res = pickle_lib.dumps(res_obj, -1)
        # pickle may contain problematic characters like newlines, so we
        # encode the pickle it as a hex string
        hex_encoded_pickle = binascii.hexlify(pickled_res)
        # encoding will be a bytestring object if in Python 3, so need to convert it to string
        # str.decode converts string to utf8 in python 2 and bytes to str in Python 3
        hex_encoded_pickle_as_str = hex_encoded_pickle.decode()
        # insert the resulting serialization into a DataFrame
        df = smvApp.createDF("pickled_result: String", hex_encoded_pickle_as_str)
        return df

    @abc.abstractmethod
    def run(self, i):
        """User-specified definition of the operations of this SmvModule

            Override this method to define the output of this module, given a map
            'i' from input SmvDataSet to resulting DataFrame. 'i' will have a
            mapping for each SmvDataSet listed in requiresDS. E.g.

            def requiresDS(self):
                return [MyDependency]

            def run(self, i):
                return train_model(i[MyDependency])

            Args:
                (RunParams): mapping from input SmvDataSet to DataFrame

            Returns:
                (object): picklable output of this SmvModule
        """

    def doRun(self, validator, known):
        i = self._constructRunParams(known)
        res_obj = self.run(i)
        result = self.result2df(self.smvApp, res_obj)
        return result._jdf


class SmvModel(SmvResultModule):
    """SmvModule whose result is a data model
    """
    # Exists only to be paired with SmvModelExec
    def dsType(self):
        return "Model"


class SmvModelExec(SmvModule):
    """SmvModule that runs a model produced by an SmvModel
    """
    def dsType(self):
        return "ModelExec"

    def dependencies(self):
        model_mod = self.requiresModel()
        if not self._targetIsSmvModel(model_mod):
            raise SmvRuntimeError("requiresModel method must return an SmvModel or a link to one")
        return [model_mod] + self.requiresDS()

    def _targetIsSmvModel(self, module):
        if isinstance(module, SmvModuleLink):
            target = module.target
        else:
            target = module

        try:
            if issubclass(target, SmvModel):
                return True
        except TypeError:
            # if target is not a class or other type object, issubclass will raise TypeError
            pass

        return False

    @abc.abstractmethod
    def requiresModel():
        """User-specified SmvModel module

            Returns:
                (SmvModel): the SmvModel this module depends on
        """

    def doRun(self, validator, known):
        i = self._constructRunParams(known)
        model = i[self.requiresModel()]
        result = self.run(i, model)
        self.assert_result_is_dataframe(result)
        return result._jdf

    @abc.abstractmethod
    def run(self, i, model):
        """User-specified definition of the operations of this SmvModule

            Override this method to define the output of this module, given a map
            'i' from inputSmvDataSet to resulting DataFrame. 'i' will have a
            mapping for each SmvDataSet listed in requiresDS. E.g.

            def requiresDS(self):
                return [MyDependency]

            def run(self, i):
                return i[MyDependency].select("importantColumn")

            Args:
                i (RunParams): mapping from input SmvDataSet to DataFrame
                model (SmvModel): the model this module depends on

            Returns:
                (object): picklable output of this SmvModule
        """


class SmvModuleLink(object):
    """A module link provides access to data generated by modules from another stage
    """

    IsSmvModuleLink = True

    def __init__(self, target):
        self.target = target

    def df2result(self, df):
        return self.target.df2result(df)

    def urn(self):
        return 'link:' + self.target.fqn()


class SmvExtDataSet(object):
    """An SmvDataSet representing an external (Scala) SmvDataSet

        E.g. MyExtMod = SmvExtDataSet("the.scala.mod")

        Args:
            fqn (str): fqn of the Scala SmvDataSet

        Returns:
            (SmvExtDataSet): external dataset with given fqn
    """
    def __init__(self, fqn):
        self._fqn = fqn

    def df2result(self, df):
        # non-DataFrame results are only supported for Python SmvResultModule
        return df

    def urn(self):
        return 'mod:' + self._fqn

    def fqn(self):
        return self._fqn


def SmvExtModuleLink(refname):
    """Creates a link to an external (Scala) SmvDataSet

        SmvExtModuleLink(fqn) is equivalent to SmvModuleLink(SmvExtDataSet(fqn))

        Args:
            fqn (str): fqn of the the Scala SmvDataSet

        Returns:
            (SmvModuleLink): link to the Scala SmvDataSet
    """
    return SmvModuleLink(SmvExtDataSet(refname))


__all__ = [
    'SmvOutput',
    'SmvModule',
    'SmvSqlModule',
    'SmvModel',
    'SmvModelExec',
    'SmvModuleLink',
    'SmvExtModuleLink'
]
