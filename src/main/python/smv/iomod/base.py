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

import abc
import importlib

from pyspark.sql import DataFrame

from smv.error import SmvRuntimeError
from smv.utils import smvhash
from smv.datasetrepo import DataSetRepo
from smv.smvgenericmodule import SmvGenericModule
from smv.smviostrategy import SmvNonOpPersistenceStrategy

class SmvIoModule(SmvGenericModule):
    """Base class for input and output modules

        Has two sub-classes:

            - SmvInput: no dependency module, single output data
            - SmvOutput: single dependency module, no output data
    """
    def isEphemeral(self):
        """SmvIoModules are always ephemeral"""
        return True

    def persistStrategy(self):
        """Never persisting input/output modules"""
        return SmvNonOpPersistenceStrategy()

    @abc.abstractmethod
    def connectionType(self):
        """Connection type supported by a specific io module"""

    @abc.abstractmethod
    def connectionName(self):
        """Name of the connection to read/write"""

    def get_connection(self):
        """Get data connection instance from connectionName()

            Connetion should be configured in conf file with at least a class FQN

            Ex: smv.conn.con_name.class=smv.conn.SmvJdbcConnectionInfo
        """
        name = self.connectionName()
        conn = self.smvApp.get_connection_by_name(name)

        # check whether the connection provided by name has the type as expected
        conn_type = conn.provider_type()
        if (conn_type != self.connectionType()):
            raise SmvRuntimeError("Connection {} has type {}, while {} need connection type {}".format(
                name, conn_type, self.__class__.__name__, self.connectionType()
            ))
        return conn

    def connectionHash(self):
        conn_hash = self.get_connection().conn_hash()
        return conn_hash


class SmvInput(SmvIoModule):
    """Base class for all Input modules

        Sub-class need to implement:

            - connectionType
            - _get_input_data

        User need to implement:

            - connectionName
    """
    def requiresDS(self):
        return []

    def dsType(self):
        return "Input"

    @abc.abstractmethod
    def _get_input_data(self):
        """Read in input to the desired typed data(frame)"""

    def doRun(self, know):
        res = self._get_input_data()
        self._assure_output_type(res)
        return res


class SmvOutput(SmvIoModule):
    """Base class for all Output modules

        Sub-class need to implement:

            - connectionType
            - doRun

        Within doRun, assert_single_input should be called.

        User need to implement:

            - connectionName
    """
    IsSmvOutput = True

    def dsType(self):
        return "Output"

    def _assert_single_input(self):
        """Make sure SmvOutput only depends on a single module
            This method will not be called, when SmvOutput is used for mixin.
            It should be called by the doRun method when SmvOutput is used for
            base class
        """
        if (len(self.requiresDS()) != 1):
            raise SmvRuntimeError("SmvOutput modules depend on a single input, more are given: {}"\
                .format(", ".join([m.fqn() for m in self.requiresDS()]))
            )

    def _do_it(self, fqn2df, run_set, collector, forceRun, is_quick_run):
        """Override _do_it, since output doRun just need to run, no other fancy stuff
        """
        self._assert_single_input()
        self.data = self.doRun(fqn2df)
        # output's doRun guarantees an action
        self._run_ancestor_and_me_postAction(run_set, collector)

class AsTable(object):
    """Mixin to assure a tableName method"""
    @abc.abstractmethod
    def tableName(self):
        """The user-specified table name to write to

            Returns:
                (string)
        """

    def tableNameHash(self):
        res = smvhash(self.tableName())
        return res

class AsFile(SmvIoModule):
    """Mixin to assure a fileName method"""
    @abc.abstractmethod
    def fileName(self):
        """User-specified file name relative to the path defined in the connection

            Returns:
                (string)
        """

    def fileNameHash(self):
        res = smvhash(self.fileName())
        return res

    def connectionType(self):
        # all files should have hdfs as their connection type
        return 'hdfs'

    def _assert_file_postfix(self, postfix):
        """Make sure that file name provided has the desired postfix"""
        if (not self.fileName().lower().endswith(postfix)):
            raise SmvRuntimeError(
                "Input file provided {} does not have postfix {}"
                .format(self.fileName(), postfix)
            )


class SmvSparkDfOutput(SmvOutput):
    """SmvOutput which write out Spark DF
    """
    def _assert_data_is_df(self, data):
        if not isinstance(data, DataFrame):
            raise SmvRuntimeError(
                "Data provided to {} has type {}, should be a DataFrame"
                .format(self.fqn(), type(data).__name__)
            )

    def get_spark_df(self, known):
        i = self.RunParams(known)

        data = i[self.requiresDS()[0]]
        self._assert_data_is_df(data)

        return data
