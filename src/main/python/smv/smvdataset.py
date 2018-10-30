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
import binascii
import json
from datetime import datetime

from smv.dqm import SmvDQM
from smv.error import SmvRuntimeError
from smv.utils import pickle_lib, lazy_property
from smv.smviostrategy import SmvCsvOnHdfsIoStrategy, SmvJsonOnHdfsIoStrategy, SmvPicklableOnHdfsIoStrategy
from smv.smvgenericmodule import SmvGenericModule, lazy_property

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

class SmvModule(SmvGenericModule):
    """Base class for SmvModules written in Python
    """

    IsSmvModule = True

    def dsType(self):
        return "Module"

    def __init__(self, smvApp):
        super(SmvModule, self).__init__(smvApp)
        self.dqmTimeElapsed = None

    #########################################################################
    # User interface methods
    #
    # Inherited from SmvGenericModule:
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
    #
    # SmvModule specific:
    # - dqm: Optional, default SmvDQM()
    # - publishHiveSql: Optional, default None
    # - run: Required
    #########################################################################
    def dqm(self):
        """DQM policy

            Override this method to define your own DQM policy (optional).
            Default is an empty policy.

            Returns:
                (SmvDQM): a DQM policy
        """
        return SmvDQM()


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

    #########################################################################
    # Implement of SmvGenericModule abatract methos and other private methods
    #########################################################################
    def doRun(self, known):
        i = self.RunParams(known)
        return self.run(i)

    def had_action(self):
        """Check dqm overall counter to simulate an action check.
            There is no way for us to tell wether the result df is just
            empty or there is no action on it. So use this with caution
        """
        return self.dqmValidator.totalRecords() > 0

    def calculate_edd(self, run_set):
        """When config smv.forceEdd flag is true, run edd calculation. 
            If already in metadata (in case persisetd), skip, otherwise
            calculate Edd and fill in to metadata, and 
            run_ancestor_and_me_postAction
        """
        current_edd = self.module_meta.getEddResult()
        if (len(current_edd) == 0):
            edd_json_array = self.smvApp._jvm.SmvPythonHelper.getEddJsonArray(self.df._jdf)
            self.run_ancestor_and_me_postAction(run_set)
            self.module_meta.addEddResult(edd_json_array)

    def force_an_action(self, df):
        # Since optimization can be done on a DF actions like count, we have to convert DF
        # to RDD and than apply an action, otherwise fix count will be always zero
        (n, self.dqmTimeElapsed) = self._do_action_on_df(
            lambda d: d.rdd.count(), df, "FORCE AN ACTION FOR DQM")

    # Override this method to add the dqmTimeElapsed 
    def finalize_meta(self):
        super(SmvModule, self).finalize_meta()
        # Need to add duration at the very end, just before persist
        self.module_meta.addDuration("dqm", self.dqmTimeElapsed)

    # Override this to add the task to a Spark job group
    def _do_action_on_df(self, func, df, desc):
        name = self.fqn()
        self.smvApp.sc.setJobGroup(groupId=name, description=desc)
        (res, secondsElapsed) = super(SmvModule, self)._do_action_on_df(func, df, desc)

        # Python api does not have clearJobGroup
        # set groupId and description to None is equivalent
        self.smvApp.sc.setJobGroup(groupId=None, description=None)
        return (res, secondsElapsed)

    def persistStrategy(self):
        return SmvCsvOnHdfsIoStrategy(self.smvApp, self.fqn(), self.ver_hex())
    
    def metaStrategy(self):
        return SmvJsonOnHdfsIoStrategy(self.smvApp, self.meta_path())

    @lazy_property
    def dqmValidator(self):
        return self.smvApp._jvm.DQMValidator(self.dqm())

    def pre_action(self, df):
        """DF in and DF out, to perform operations on created from run method"""
        return DataFrame(self.dqmValidator.attachTasks(df._jdf), df.sql_ctx)

    def post_action(self):
        """Will run when action happens on a DF, here for DQM validation"""
        validation_result = self.dqmValidator.validate()
        if (not validation_result.isEmpty()):
            msg = json.dumps(
                json.loads(validation_result.toJSON()),
                indent=2, separators=(',', ': ')
            )
            self.smvApp.log.warn("Nontrivial DQM result:\n{}".format(msg))
        self.module_meta.addDqmValidationResult(validation_result.toJSON())

    # All publish related methods should be moved to generic output module class
    def exportToHive(self):
        # if user provided a publish hive sql command, run it instead of default
        # table creation from data frame result.
        if (self.publishHiveSql() is None):
            queries = [
                "drop table if exists {}".format(self.tableName()),
                "create table {} as select * from dftable".format(self.tableName())
            ]
        else:
            queries = [l.strip() for l in self.publishHiveSql().split(";")]

        self.smvApp.log.info("Hive publish query: {}".format(";".join(queries)))
        def run_query(df):
            # register the dataframe as a temp table.  Will be overwritten on next register.
            df.createOrReplaceTempView("dftable")
            for l in queries:
                self.smvApp.sqlContext.sql(l)
        
        self._do_action_on_df(run_query, self.df, "PUBLISH TO HIVE")
    
    def publishThroughJDBC(self):
        url = self.smvApp.jdbcUrl()
        driver = self.smvApp.jdbcDriver()
        self.smvApp.j_smvPyClient.writeThroughJDBC(self.df._jdf, url, driver, self.tableName())

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
    def persistStrategy(self):
        return SmvPicklableOnHdfsIoStrategy(self.smvApp, self.fqn(), self.ver_hex())

    @classmethod
    def df2result(self, df):
        """Unpickle and decode module result stored in DataFrame
        """
        return df

    @classmethod
    def result2df(cls, smvApp, res_obj):
        """Pick and encode module result, and store it in a DataFrame
        """
        return res_obj

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

    def doRun(self, known):
        i = self.RunParams(known)
        return self.run(i)


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

    def doRun(self, known):
        i = self.RunParams(known)
        model = i[self.requiresModel()]
        return self.run(i, model)

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
