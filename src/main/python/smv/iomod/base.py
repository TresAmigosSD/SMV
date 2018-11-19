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
from smv.smvgenericmodule import SmvGenericModule
from smv.smviostrategy import SmvNonOpPersistenceStrategy, SmvJsonOnHdfsPersistenceStrategy


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

    def metaStrategy(self):
        """Still persist meta for input/output modules"""
        return SmvJsonOnHdfsPersistenceStrategy(self.smvApp, self.meta_path())

    @abc.abstractmethod
    def connectionName(self):
        """Name of the connection to read/write"""

    def _get_connection_by_name(self, name):
        """Get connection instance from name
        """
        props = self.smvApp.py_smvconf.merged_props()
        class_key = "smv.conn.{}.class".format(name)

        if (class_key in props):
            con_class = props.get(class_key)
            # Load the class from its FQN
            module_name, class_name = con_class.rsplit(".", 1)
            ConnClass = getattr(importlib.import_module(module_name), class_name)
            return ConnClass(name, props)
        else:
            raise SmvRuntimeError("Connection name {} is not configured with a class".format(name))

    def get_connection(self):
        """Get data connection instance from connectionName()

            Connetion should be configured in conf file with at least a class FQN

            Ex: smv.conn.con_name.class=smv.conn.SmvJdbcConnectionInfo
        """
        name = self.connectionName()
        return self._get_connection_by_name(name)


class SmvInput(SmvIoModule):
    """Base class for all Input modules

        Sub-class need to implement:

            - doRun

        User need to implement:

            - connectionName
    """
    def requiresDS(self):
        return []

    def dsType(self):
        return "Input"

    def instanceValHash(self):
        """TODO: need to implement this to depends on connection and
            also table itself"""
        return 0

class SmvOutput(SmvIoModule):
    """Base class for all Output modules

        Sub-class need to implement:

            - doRun

        Within doRun, assert_single_input should be called.

        User need to implement:

            - connectionName
    """
    IsSmvOutput = True

    def dsType(self):
        return "Output"

    def assert_single_input(self):
        """Make sure SmvOutput only depends on a single module
            This method will not be called, when SmvOutput is used for mixin.
            It should be called by the doRun method when SmvOutput is used for
            base class
        """
        if (len(self.requiresDS()) != 1):
            raise SmvRuntimeError("SmvOutput modules depend on a single input, more are given: {}"\
                .format(", ".join([m.fqn() for m in self.requiresDS()]))
            )


class AsTable(object):
    """Mixin to assure a tableName method"""
    @abc.abstractmethod
    def tableName(self):
        """The user-specified table name to write to

            Returns:
                (string)
        """


class AsFile(object):
    """Mixin to assure a fileName method"""
    @abc.abstractmethod
    def fileName(self):
        """User-specified file name relative to the path defined in the connection

            Returns:
                (string)
        """

    def _assert_file_postfix(self, postfix):
        """Make sure that file name provided has the desired postfix"""
        if (not self.fileName().endswith(postfix)):
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
        self.assert_single_input()
        i = self.RunParams(known)

        data = i[self.requiresDS()[0]]
        self._assert_data_is_df(data)

        return data
