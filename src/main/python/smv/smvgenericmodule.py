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
import abc
import inspect
import sys
import traceback
import binascii
import json
from datetime import datetime

from smv.utils import lazy_property
from smv.error import SmvRuntimeError
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


class SmvGenericModule(ABC):
    """Abstract base class for all SMV modules, including dataset and task modules
    """

    # TODO: Change the name to IsSmvGenericModule
    # Python's issubclass() check does not work well with dynamically
    # loaded modules.  In addition, there are some issues with the
    # check, when the `abc` module is used as a metaclass, that we
    # don't yet quite understand.  So for a workaround we add the
    # typcheck in the Smv hierarchies themselves.
    IsSmvDataSet = True

    def __init__(self, smvApp):
        self.smvApp = smvApp

        # Set when instant created and resolved 
        self.timestamp = None
        self.resolvedRequiresDS = []

        # keep a reference to the result data
        self.data = None

        self.module_meta = SmvMetaData()
        self.userMetadataTimeElapsed = None
        self.persistingTimeElapsed = None

    @classmethod
    def fqn(cls):
        """Returns the fully qualified name
        """
        return cls.__module__ + "." + cls.__name__

    @classmethod
    def urn(cls):
        return "mod:" + cls.fqn()

    #########################################################################
    # User interface methods
    #
    # - isEphemeral: Optional, default False
    # - description: Optional, default class docstr
    # - requiresDS: Required 
    # - requiresConfig: Optional, default []
    # - requiresLib: Optional, default []
    # - metadata: Optional, default {}
    # - validateMetadata: Optional, default None
    # - metadataHistorySize: Optional, default 5
    # - version: Optional, default "0" --- Deprecated!
    #########################################################################

    def isEphemeral(self):
        """Should this SmvDataSet skip persisting its data?

            Returns:
                (bool): True if this SmvDataSet should not persist its data, false otherwise
        """
        return False

    def description(self):
        return self.__doc__

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

    def metadataHistorySize(self):
        """Override to define the maximum size of the metadata history for this module

            Return:
                (int): size
        """
        return 5

    def version(self):
        """Version number
            Deprecated!

            Returns:
                (str): version number of this SmvDataSet
        """
        return "0"


    ####################################################################################
    # Methods can be called from client code (in run method)
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


    #########################################################################
    # Methods for sub-classes to implement and/or override
    #
    # - dsType: Required
    # - persistStrategy: Required
    # - metaStrategy: Required
    # - doRun: Optional, default call run 
    # - dependencies: Optional, default self.requiresDS()
    # - df2result: Optional, default pass though the input data
    # - had_action: Optional, default True
    # - pre_action: Optional, default pass through the input data
    # - post_action: Optional, default pass
    # - force_an_action: Optional, default pass
    # - calculate_edd: Optional, default pass
    # - instanceValHash: Optional, default 0
    #########################################################################
    @abc.abstractmethod
    def dsType(self):
        """Return SmvDataSet's type"""

    @abc.abstractmethod
    def persistStrategy(self):
        """Return an SmvIoStrategy for data persisting"""
    
    @abc.abstractmethod
    def metaStrategy(self):
        """Return an SmvIoStrategy for metadata persisting"""

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
                # called df2result so that SmvModel result get returned in `i`
                return ds.df2result(self.urn2df[ds.urn()])

    def doRun(self, known):
        """Compute this dataset, and return the dataframe"""
        i = self.RunParams(known)
        return self.run(i)
    
    def dependencies(self):
        """Can be overridden when a module has non-SmvDataSet dependencies (see SmvModelExec)
        """
        return self.requiresDS()

    @classmethod
    def df2result(cls, df):
        """Given a datasets's persisted DataFrame, get the result object

            In most cases, this is just the DataFrame itself. See SmvResultModule for the exception.
        """
        return df

    def had_action(self):
        """Check whether there is an action happend on the generated data (DF or real data)
            For Spark DF and other types of lazy-eval data, this method need to be set through 
            so data specific logic. Return True is the DF is evaled. For data type which does 
            not do lazy eval, is one always return True
        """
        return True

    def pre_action(self, df):
        """DF in and DF out, to perform operations on created from run method"""
        return df

    def post_action(self):
        """Will run when action happens on a DF, here for DQM validation and others 
            which need to be done after an action on lazy-eval data"""
        pass

    def force_an_action(self, df):
        """For Spark DF and other data with lazy-eval, may need to force an action to 
            trigger the post_action calculation. For general data without lazy-eval, do nothing
        """
        pass

    def calculate_edd(self, run_set):
        """When config smv.forceEdd flag is true, run edd calculation. 
            So far only Spark DF has edd defined
        """
        pass

    def instanceValHash(self):
        """Hash computed based on instance values of the dataset, such as the timestamp of an input file
        """
        return 0
    

    ####################################################################################
    # Private methods: not expect to be overrided by sub-classes, but could be
    ####################################################################################

    # Set when resolving
    def setTimestamp(self, dt):
        self.timestamp = dt

    # Called by resolver, recursively resolve all dependencies. Use self.dependencies 
    # instead of requiresDS to make sure model dependency also included
    def resolve(self, resolver):
        self.resolvedRequiresDS = resolver.loadDataSet([ds.fqn() for ds in self.dependencies()])
        return self


    def rdd(self, urn2df, run_set, forceRun, is_quick_run):
        """create or get df from smvApp level cache
            Args:
                urn2df({str:DataFrame}) already run modules current module may depends
                run_set(set(SmvDataSet)) modules yet to run post_action
                forceRun(bool) ignore DF cache in smvApp
                is_quick_run(bool) skip meta, dqm, persist, but use persisted as possible

            urn2df will be appended, and run_set will shrink
        """
        if (forceRun or (self.versioned_fqn not in self.smvApp.df_cache)):
            self.smvApp.df_cache.update(
                {self.versioned_fqn:self.computeDataFrame(urn2df, run_set, is_quick_run)}
            )
        else:
            run_set.discard(self)
        res = self.smvApp.df_cache.get(self.versioned_fqn)
        urn2df.update({self.urn(): res})
        self.data = res

    def computeDataFrame(self, urn2df, run_set, is_quick_run):
        """When DF is not in cache, do the real calculation here
        """
        self.smvApp.log.debug("compute: {}".format(self.urn()))

        if (self.isEphemeral()):
            raw_df = self.doRun(urn2df)
            return self.pre_action(raw_df)
        elif(is_quick_run):
            _strategy = self.persistStrategy()
            if (not _strategy.isPersisted()):
                return self.doRun(urn2df)
            else:
                return _strategy.read()
        else:
            _strategy = self.persistStrategy()
            if (not _strategy.isPersisted()):
                raw_df = self.doRun(urn2df)
                df = self.pre_action(raw_df)
                (res, self.persistingTimeElapsed) = self._do_action_on_df(
                    _strategy.write, df, "RUN & PERSIST OUTPUT")
                # There is a chance that when waiting lock in 
                # _strategy.write, another process persisted the same module
                # current module's persisting is canceled, so need to check
                # whether there is an action
                if (self.had_action()):
                    self.run_ancestor_and_me_postAction(run_set)
            else:
                run_set.discard(self)
            return _strategy.read()

    def calculate_user_meta(self, run_set):
        """Calculate user defined metadata
            could have action on the result df
        """
        io_strategy = self.metaStrategy()
        if (not io_strategy.isPersisted()):
            self.module_meta.addSystemMeta(self)
            (user_meta, self.userMetadataTimeElapsed) = self._do_action_on_df(
                self.metadata, self.data, "GENERATE USER METADATA")

            if not isinstance(user_meta, dict):
                raise SmvRuntimeError("User metadata {} is not a dict".format(repr(user_meta)))

            self.module_meta.addUserMeta(user_meta)
            if (self.had_action()):
                self.smvApp.log.debug("{} metadata had an action".format(self.fqn()))
                self.run_ancestor_and_me_postAction(run_set)
        else:
            meta_json = io_strategy.read()
            self.module_meta = SmvMetaData().fromJson(meta_json)
            # if meta persisted, no need to run post_action again
            run_set.discard(self)

    def finalize_meta(self):
        # Need to add duration at the very end, just before persist
        self.module_meta.addDuration("persisting", self.persistingTimeElapsed)
        self.module_meta.addDuration("metadata", self.userMetadataTimeElapsed)

    def persist_meta(self):
        meta_json = self.module_meta.toJson()
        self.metaStrategy().write(meta_json)

    def get_metadata(self):
        """Return the best meta without run. If persisted, use it, otherwise
            add info up to resolved DS"""
        io_strategy = self.metaStrategy()
        persisted = io_strategy.isPersisted()
        if (not persisted):
            self.module_meta.addSystemMeta(self)
            return self.module_meta
        else:
            meta_json = io_strategy.read()
            return SmvMetaData().fromJson(meta_json)

    def force_post_action(self, run_set):
        if (self in run_set):
            self.force_an_action(self.data)
            self.run_ancestor_and_me_postAction(run_set)

    def run_ancestor_and_me_postAction(self, run_set):
        """When action happens on current module, run the the delayed 
            post action of the ancestor ephemeral modules
        """
        def run_delayed_postAction(mod, _run_set):
            if (mod in _run_set):
                mod.post_action()
                _run_set.discard(mod)
        ModulesVisitor([self]).dfs_visit(run_delayed_postAction, run_set)

    def _do_action_on_df(self, func, df, desc):
        log = self.smvApp.log
        log.info("STARTING " + desc)

        before  = datetime.now()

        res = func(df)

        after   = datetime.now()
        duration = (after - before)
        secondsElapsed = duration.total_seconds()

        log.info("COMPLETED {}: {}".format(desc, self.fqn()))
        log.info("RunTime: {}".format(duration))

        return (res, secondsElapsed)

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

    def needsToRun(self):
        """For non-ephemeral module, when persisted, no need to run
            for ephemeral module if all its requiresDS no need to run, 
            also no need to run
        """
        if (self.isEphemeral()):
            for m in self.resolvedRequiresDS:
                if m.needsToRun():
                    return True
            return False
        else:
            return not self.persistStrategy().isPersisted()

    def isSmvOutput(self):
        try:
            return self.IsSmvOutput
        except:
            return False

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
