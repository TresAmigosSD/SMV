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
from smv.smviostrategy import SmvNonOpIoStrategy
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
    def connection_name(self):
        """Name of the connection to read/write"""
    
    def get_connection(self):
        """Get connection instance from name"""
        name = self.connection_name()
        props = self.smvApp.py_smvconf.merged_props()
        type_key = "smv.con.{}.type".format(name)
        con_type = props.get(type_key).lower()
        if (con_type == "jdbc"):
            return SmvJdbcConnectionInfo(name, props)
        else:
            raise SmvRuntimeError("Connection type {} is not implemented".format(con_type))


class SmvJdbcInputTable(SmvIoModule):
    """
        User need to implement 

            - connection_name
            - table_name
    """

    @abc.abstractmethod
    def table_name(self):
        """
        """

    def requiresDS(self):
        return []

    def dsType(self):
        return "Input"

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
            .option('dbtable', self.table_name())\
            .load()