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
from smv.smviostrategy import SmvCsvOnHdfsIoStrategy, SmvJsonOnHdfsIoStrategy
from smv.modulesvisitor import ModulesVisitor
from smv.smvmetadata import SmvMetaData

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

def lazy_property(fn):
    '''Decorator that makes a property lazy-evaluated.
    '''
    attr_name = '_lazy_' + fn.__name__

    @property
    def _lazy_property(self):
        if not hasattr(self, attr_name):
            setattr(self, attr_name, fn(self))
        return getattr(self, attr_name)
    return _lazy_property

class SmvOutput(object):
    """Mixin which marks an SmvModule as one of the output of its stage

        SmvOutputs are distinct from other SmvDataSets in that
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

        # For #1417, python side resolving (not used yet)
        self.timestamp = None
        self.resolvedRequiresDS = []

        # keep a reference to the result DF
        self.df = None

        self.module_meta = SmvMetaData()
        self.userMetadataTimeElapsed = None
        self.persistingTimeElapsed = None
        self.dqmTimeElapsed = None

    # For #1417, python side resolving (not used yet)
    def setTimestamp(self, dt):
        self.timestamp = dt

    # For #1417, python side resolving (not used yet)
    def resolve(self, resolver):
        self.resolvedRequiresDS = resolver.loadDataSet([ds.fqn() for ds in self.requiresDS()])
        return self

    ####################################################################################
    # Will eventually move to the SmvGenericModule base class
    ####################################################################################
    def rdd(self, urn2df, run_set):
        """create or get df from smvApp level cache
            Args:
                urn2df({str:DataFrame}) already run modules current module may depends
                run_set(set(SmvDataSet)) modules yet to run post_action

            urn2df will be appended, and run_set will shrink
        """
        if (self.versioned_fqn not in self.smvApp.df_cache):
            self.smvApp.df_cache.update(
                {self.versioned_fqn:self.computeDataFrame(urn2df, run_set)}
            )
        else:
            run_set.discard(self)
        res = self.smvApp.df_cache.get(self.versioned_fqn)
        urn2df.update({self.urn(): res})
        self.df = res

    def computeDataFrame(self, urn2df, run_set):
        self.smvApp.log.debug("compute: {}".format(self.urn()))

        if (self.isEphemeral()):
            raw_df = self.doRun2(urn2df)
            return self.pre_action(raw_df)
        else:
            _strategy = self.persistStrategy()
            if (not _strategy.isPersisted()):
                raw_df = self.doRun2(urn2df)
                df = self.pre_action(raw_df)
                (res, self.persistingTimeElapsed) = self._do_action_on_df(
                    _strategy.write, df, "RUN & PERSIST OUTPUT")
                self.run_ancestor_and_me_postAction(run_set)
            else:
                run_set.discard(self)
            return _strategy.read()

    def had_action(self):
        return self.dqmValidator.totalRecords() > 0

    def calculate_user_meta(self, run_set):
        if (not self.metaStrategy().isWritten()):
            self.module_meta.addSystemMeta(self)
            (user_meta, self.userMetadataTimeElapsed) = self._do_action_on_df(
                self.metadata, self.df, "GENERATE USER METADATA")
            self.module_meta.addUserMeta(user_meta)
            if (self.had_action()):
                self.smvApp.log.debug("{} metadata had an action".format(self.fqn()))
                self.run_ancestor_and_me_postAction(run_set)
        else:
            # if meta persisted, no need to run post_action again
            run_set.discard(self)

    def force_an_action(self, df):
        (n, self.dqmTimeElapsed) = self._do_action_on_df(
            lambda d: d.count(), df, "FORCE AN ACTION FOR DQM")

    def persist_meta(self):
        if (not self.metaStrategy().isWritten()):
            # Need to add duration at the very end, just before persist
            self.module_meta.addDuration("persisting", self.persistingTimeElapsed)
            self.module_meta.addDuration("metadata", self.userMetadataTimeElapsed)
            self.module_meta.addDuration("dqm", self.dqmTimeElapsed)
            meta_json = self.module_meta.toJson()
            self.metaStrategy().write(meta_json)

    def force_post_action(self, run_set):
        if (self in run_set):
            self.force_an_action(self.df)
            self.run_ancestor_and_me_postAction(run_set)

    def run_ancestor_and_me_postAction(self, run_set):
        def run_delayed_postAction(mod, _run_set):
            if (mod in _run_set):
                mod.post_action()
                _run_set.discard(mod)
        ModulesVisitor([self]).dfs_visit(run_delayed_postAction, run_set)

    def _do_action_on_df(self, func, df, desc):
        log = self.smvApp.log
        log.info("STARTING " + desc)

        name = self.fqn()
        self.smvApp.sc.setJobGroup(groupId=name, description=desc)
        before  = datetime.now()

        res = func(df)

        after   = datetime.now()
        duration = (after - before)
        secondsElapsed = duration.total_seconds()

        # Python api does not have clearJobGroup
        # set groupId and description to None is equivalent
        self.smvApp.sc.setJobGroup(groupId=None, description=None)

        log.info("COMPLETED {}: {}".format(desc, name))
        log.info("RunTime: {}".format(duration))

        return (res, secondsElapsed)

    # Make this not an abstract since not all concrete class has this
    #@abc.abstractmethod
    def doRun2(self, known):
        """Compute this dataset, and return the dataframe"""
        pass

    def dataset_hash(self): 
        """current module's hash value, depend on code and potentially 
            linked data (such as for SmvCsvFile)
        """
        log = self.smvApp.log
        _instanceValHash = self.instanceValHash()
        log.debug("{}.instanceValHash = {}".format(self.fqn(), _instanceValHash))

        _sourceCodeHash = self.sourceCodeHash()
        log.debug("{}.sourceCodeHash = ${}".format(self.fqn(), _sourceCodeHash))

        return _instanceValHash + _sourceCodeHash
    
    @lazy_property
    def hash_of_hash(self):
        """hash depends on current module's dataset_hash, and all ancestors.
            this calculation could be expensive, so made it a lazy property
        """
        # TODO: implement using visitor too
        log = self.smvApp.log
        _dataset_hash = self.dataset_hash()
        log.debug("{}.dataset_hash = {}".format(self.fqn(), _dataset_hash))
    
        res = _dataset_hash
        for m in self.resolvedRequiresDS:
            res += m.hash_of_hash 
        log.debug("{}.hash_of_hash = {}".format(self.fqn(), res))
        return res
    
    def ver_hex(self):
        return "{0:08x}".format(self.hash_of_hash)

    @lazy_property
    def versioned_fqn(self):
        """module fqn with the hash of hash. It is the signature of a specific
            version of the module
        """
        return "{}_{}".format(self.fqn(), self.ver_hex())

    def meta_path(self):
        return "{}/{}.meta".format(
            self.smvApp.all_data_dirs().outputDir,
            self.versioned_fqn)

    def persistStrategy(self):
        return SmvCsvOnHdfsIoStrategy(self.smvApp, self.fqn(), self.ver_hex())
    
    def metaStrategy(self):
        return SmvJsonOnHdfsIoStrategy( self.smvApp, self.meta_path())

    @lazy_property
    def dqmValidator(self):
        return self.smvApp._jvm.DQMValidator(self.dqmWithTypeSpecificPolicy())

    def pre_action(self, df):
        return DataFrame(self.dqmValidator.attachTasks(df._jdf), df.sql_ctx)

    def post_action(self):
        validation_result = self.dqmValidator.validate(None, True)
        self.module_meta.addDqmValidationResult(validation_result.toJSON())

    def needsToRun(self):
        if (self.isEphemeral()):
            return False
        else:
            return not self.persistStrategy().isWritten()

    def isSmvOutput(self):
        try:
            return self.IsSmvOutput
        except:
            return False

    # All publish related methods should be moved to generic output module class

    ####################################################################################
    def smvGetRunConfig(self, key):
        """return the current user run configuration value for the given key."""
        if (key not in self.requiresConfig()):
            raise SmvRuntimeError("RunConfig key {} was not specified in requiresConfig method{}.".format(key, self.requiresConfig()))

        return self.smvApp.getConf(key)
    
    def smvGetRunConfigAsInt(self, key):
        runConfig = self.smvGetRunConfig(key)
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
        try:
            self._is_smv_run_config()
            is_run_conf = True
        except:
            is_run_conf = False

        if (is_run_conf):
            return self.smvApp.py_smvconf.get_run_config_keys()
        else:
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
        res = 0

        cls = self.__class__
        # get hash of module's source code text
        try:
            sourceHash = _sourceHash(cls)
        except Exception as err:  # `inspect` will raise error for classes defined in the REPL
            # Instead of handle the case that module defined in REPL, just raise Exception here
            # res = _smvhash(_disassemble(cls))
            traceback.print_exc()
            message = "{0}({1!r})".format(type(err).__name__, err.args)
            raise Exception(
                message + "\n" + "SmvDataSet " +
                self.urn() + " defined in shell can't be persisted"
            )

        self.smvApp.log.debug("{} sourceHash: {}".format(self.fqn(), sourceHash))
        res += sourceHash

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
        config_hash = self.config_hash()
        self.smvApp.log.debug("{} config_hash: {}".format(self.fqn(), config_hash))
        res += config_hash

        # iterate through libs/modules that this DataSet depends on and use their source towards hash as well
        for lib in self.requiresLib():
            lib_src_hash = _sourceHash(lib)
            self.smvApp.log.debug("{} sourceHash: {}".format(lib.__name__, lib_src_hash))
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
            historical_keys_hash = sum(keys_hash)
            self.smvApp.log.debug("{} historical keys hash: {}".format(historical_keys_hash))
            res += historical_keys_hash

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
                def requiresDS(self): return [Foo]
                def run(self, i): return i[Foo]

            the i argument of the run method should map Foo to
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

    def doRun2(self, known):
        i = self.RunParams(known)
        return self.run(i)

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

    def _targetIsSmvModel(self, target):
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


def SmvModuleLink(target):
    return target


__all__ = [
    'SmvOutput',
    'SmvModule',
    'SmvSqlModule',
    'SmvModel',
    'SmvModelExec',
    'SmvModuleLink'
]
