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

This module defines the abstract classes which formed the SmvModule Framework for clients' projects
"""

from pyspark.sql import DataFrame

import abc
import binascii
import json
from datetime import datetime

import smv
from smv.dqm import SmvDQM
from smv.error import SmvRuntimeError
from smv.utils import pickle_lib, lazy_property
from smv.smviostrategy import SmvCsvPersistenceStrategy, SmvPicklablePersistenceStrategy, SmvParquetPersistenceStrategy
from smv.smvgenericmodule import SmvProcessModule, SmvGenericModule

class SmvOutput(object):
    """Mixin which marks an SmvModule as one of the output of its stage
        SmvOutputs are distinct from other SmvModule in that

            * The -s and --run-app options of smv-run only run SmvOutputs and their dependencies.

        Deprecated. Will be replaced by sub-classed of smv.io.SmvOutput.
    """
    IsSmvOutput = True

    def tableName(self):
        """The user-specified table name used when exporting data to Hive (optional)
            Returns:
                (string)
        """
        return None


class SparkDfGenMod(SmvGenericModule):
    """Base class for SmvModules create Spark DFs
    """

    def __init__(self, smvApp):
        super(SparkDfGenMod, self).__init__(smvApp)
        self.dqmTimeElapsed = None

    #########################################################################
    # User interface methods
    #
    # SparkDfGenMod specific:
    # - dqm: Optional, default SmvDQM()
    #########################################################################
    def dqm(self):
        """DQM policy

            Override this method to define your own DQM policy (optional).
            Default is an empty policy.

            Returns:
                (SmvDQM): a DQM policy
        """
        return SmvDQM()

    #########################################################################
    # Implement of SmvGenericModule abatract methos and other private methods
    #########################################################################
    def _calculate_edd(self):
        """When config smv.forceEdd flag is true, run edd calculation.
        """
        def get_edd(df):
            return self.smvApp._jvm.SmvPythonHelper.getEddJsonArray(df._jdf)

        (edd_json_array, eddTimeElapsed) = self._do_action_on_df(
            get_edd, self.data, "CALCULATE EDD")
        self.module_meta.addEddResult(edd_json_array)
        self.module_meta.addDuration("edd", eddTimeElapsed)

    def _force_an_action(self, df):
        # Since optimization can be done on a DF actions like count, we have to convert DF
        # to RDD and than apply an action, otherwise fix count will be always zero
        (n, self.dqmTimeElapsed) = self._do_action_on_df(
            lambda d: d.rdd.count(), df, "FORCE AN ACTION FOR DQM")

    # Override this method to add the edd calculation if config
    def _calculate_user_meta(self):
        super(SparkDfGenMod, self)._calculate_user_meta()
        if (self.smvApp.py_smvconf.force_edd()):
            self._calculate_edd()

    # Override this method to add the dqmTimeElapsed
    def _finalize_meta(self):
        super(SparkDfGenMod, self)._finalize_meta()
        self.module_meta.addSchemaMetadata(self.data)
        # Need to add duration at the very end, just before persist
        self.module_meta.addDuration("dqm", self.dqmTimeElapsed)

    # Override this to add the task to a Spark job group
    def _do_action_on_df(self, func, df, desc):
        name = self.fqn()
        self.smvApp.sc.setJobGroup(groupId=name, description=desc)
        (res, secondsElapsed) = super(SparkDfGenMod, self)._do_action_on_df(func, df, desc)

        # Python api does not have clearJobGroup
        # set groupId and description to None is equivalent
        self.smvApp.sc.setJobGroup(groupId=None, description=None)
        return (res, secondsElapsed)

    def persistStrategy(self):
        _format = self.smvApp.py_smvconf.df_persist_format()
        if (_format == "smvcsv_on_hdfs"):
            return SmvCsvPersistenceStrategy(self.smvApp, self.versioned_fqn)
        elif (_format == "parquet_on_hdfs"):
            return SmvParquetPersistenceStrategy(self.smvApp, self.versioned_fqn)

    @lazy_property
    def _dqmValidator(self):
        return self.smvApp._jvm.DQMValidator(self.dqm())

    def _pre_action(self, df):
        """DF in and DF out, to perform operations on created from run method"""
        return DataFrame(self._dqmValidator.attachTasks(df._jdf), df.sql_ctx)

    def _post_action(self):
        """Will run when action happens on a DF, here for DQM validation"""
        validation_result = self._dqmValidator.validate()
        if (not validation_result.isEmpty()):
            msg = json.dumps(
                json.loads(validation_result.toJSON()),
                indent=2, separators=(',', ': ')
            )
            smv.logger.warn("Nontrivial DQM result:\n{}".format(msg))
        self.module_meta.addDqmValidationResult(validation_result.toJSON())

    def _assure_output_type(self, run_output):
        if (not isinstance(run_output, DataFrame)):
            raise SmvRuntimeError(
                'The output data from this module should be a Spark DataFrame, but {} is given.'.format(type(run_output))
            )

class SmvSparkDfModule(SmvProcessModule, SparkDfGenMod):
    """Base class for SmvModules create Spark DFs
    """

    IsSmvModule = True

    def dsType(self):
        return "Module"

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
    # SmvSparkDfModule specific:
    # - dqm: Optional, default SmvDQM()
    # - publishHiveSql: Optional, default None
    # - run: Required
    #########################################################################

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

        smv.logger.info("Hive publish query: {}".format(";".join(queries)))
        def run_query(df):
            # register the dataframe as a temp table.  Will be overwritten on next register.
            df.createOrReplaceTempView("dftable")
            for l in queries:
                self.smvApp.sqlContext.sql(l)

        self._do_action_on_df(run_query, self.data, "PUBLISH TO HIVE")

    def publishThroughJDBC(self):
        url = self.smvApp.jdbcUrl()
        driver = self.smvApp.jdbcDriver()
        self.smvApp.j_smvPyClient.writeThroughJDBC(self.data._jdf, url, driver, self.tableName())

class SmvModule(SmvSparkDfModule):
    """SmvModule is the same as SmvSparkDfModule. Since it was used in all the
        SMV projects, we will not change the its name for backward compatibility.
        May deprecate when the full SmvGenericModule framework get adopted
    """
    pass


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


class SmvModel(SmvProcessModule):
    """SmvModule whose result is a data model

        The result must be picklable - see
        https://docs.python.org/2/library/pickle.html#what-can-be-pickled-and-unpickled.
    """
    # Exists only to be paired with SmvModelExec
    def dsType(self):
        return "Model"

    def persistStrategy(self):
        return SmvPicklablePersistenceStrategy(self.smvApp, self.versioned_fqn)


class SmvModelExec(SmvModule):
    """SmvModule that runs a model produced by an SmvModel
    """
    def dsType(self):
        return "ModelExec"

    def _dependencies(self):
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
            'i' from input SmvGenericModule to resulting DataFrame. 'i' will have a
            mapping for each SmvGenericModule listed in requiresDS. E.g.

            def requiresDS(self):
                return [MyDependency]

            def run(self, i):
                return i[MyDependency].select("importantColumn")

            Args:
                i (RunParams): mapping from input SmvGenericModule to DataFrame
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
