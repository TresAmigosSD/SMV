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

from smv.error import SmvRuntimeError
from smv.smvgenericmodule import SmvGenericModule
from smv.smviostrategy import SmvNonOpIoStrategy, SmvJsonOnHdfsIoStrategy
from smv.smvconnectioninfo import SmvJdbcConnectionInfo


class SmvIoModule(SmvGenericModule):
    def isEphemeral(self):
        """SmvIoModules are always ephemeral"""
        return True

    def persistStrategy(self):
        return SmvNonOpIoStrategy()

    def metaStrategy(self):
        return SmvJsonOnHdfsIoStrategy(self.smvApp, self.meta_path())

    @abc.abstractmethod
    def connectionName(self):
        """Name of the connection to read/write"""
    
    def get_connection(self):
        """Get connection instance from name"""
        name = self.connectionName()
        props = self.smvApp.py_smvconf.merged_props()
        type_key = "smv.con.{}.type".format(name)
        con_type = props.get(type_key).lower()
        if (con_type == "jdbc"):
            return SmvJdbcConnectionInfo(name, props)
        else:
            raise SmvRuntimeError("Connection type {} is not implemented".format(con_type))


class SmvInput(SmvIoModule):
    def requiresDS(self):
        return []

    def dsType(self):
        return "Input"

    @abc.abstractmethod
    def tableName(self):
        """The user-specified table name to read in from the connection

            Returns:
                (string)
        """

class SmvOutput(SmvIoModule):
    """Reuse the output marker mixin, should work for old mixin and new base class.
        Will make it symmetric to SmvInput when mixin usage is deprecated.
        
        Mixin which marks an SmvModule as one of the output of its stage

        SmvOutputs are distinct from other SmvModule in that
            * The -s and --run-app options of smv-run only run SmvOutputs and their dependencies.
    """
    IsSmvOutput = True

    def dsType(self):
        return "Output"

    def tableName(self):
        """The user-specified table name to write to

            Returns:
                (string)
        """
        # Implemented as None to be backword compatable as the mixin marker
        return None

    def connectionName(self):
        # Implemented as None to be backword compatable as the mixin marker
        return None
    
    def assert_single_input(self):
        if (len(self.requiresDS()) != 1):
            raise SmvRuntimeError("SmvOutput modules depend on a single input, more are given: {}"\
                .format(", ".join([m.fqn() for m in self.requiresDS()]))
            )


class SmvJdbcInputTable(SmvInput):
    """
        User need to implement 

            - connectionName
            - tableName
    """

    def doRun(self, known):
        conn = self.get_connection()
        builder = self.smvApp.sqlContext.read\
            .format('jdbc')\
            .option('url', conn.url)

        if (conn.driver is not None):
            builder = builder.option('driver', conn.driver)
        if (conn.user is not None):
            builder = builder.option('user', conn.user)
        if (conn.password is not None):
            builder = builder.option('password', conn.password)

        return builder\
            .option('dbtable', self.tableName())\
            .load()


class SmvJdbcOutputTable(SmvOutput):
    """
        User need to implement 

            - requiresDS
            - connectionName
            - tableName
            - writeMode: optional, default "errorifexists"
    """

    def writeMode(self):
        """
            Write mode for Spark DataFrameWriter.
            Valid values:

                - "append"
                - "overwrite"
                - "ignore"
                - "error" or "errorifexists" (default)
        """
        return "errorifexists"


    def doRun(self, known):
        self.assert_single_input()
        i = self.RunParams(known)

        conn = self.get_connection()
        data = i[self.requiresDS()[0]]

        builder = data.write\
            .format("jdbc") \
            .mode(self.writeMode()) \
            .option('url', conn.url)

        if (conn.driver is not None):
            builder = builder.option('driver', conn.driver)
        if (conn.user is not None):
            builder = builder.option('user', conn.user)
        if (conn.password is not None):
            builder = builder.option('password', conn.password)

        builder \
            .option("dbtable", self.tableName()) \
            .save()

        # return data back for meta calculation
        # TODO: need to review whether we should even calculate meta for output
        return data


__all__ = [
    'SmvJdbcInputTable',
    'SmvJdbcOutputTable',
    'SmvOutput',
]