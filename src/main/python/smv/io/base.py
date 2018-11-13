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

from smv.error import SmvRuntimeError
from smv.smvgenericmodule import SmvGenericModule
from smv.smviostrategy import SmvNonOpIoStrategy, SmvJsonOnHdfsIoStrategy


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
        return SmvNonOpIoStrategy()

    def metaStrategy(self):
        """Still persist meta for input/output modules"""
        return SmvJsonOnHdfsIoStrategy(self.smvApp, self.meta_path())

    @abc.abstractmethod
    def connectionName(self):
        """Name of the connection to read/write"""
    
    def get_connection(self):
        """Get connection instance from name
            
            Connetion should be configured in conf file with at least a class FQN

            Ex: smv.conn.con_name.class=smv.conn.SmvJdbcConnectionInfo
        """
        name = self.connectionName()
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


class SmvInput(SmvIoModule):
    """Base class for all Input modules

        Sub-class need to implement:

            - doRun
    
        User need to implement:

            - connectionName
            - tableName
    """
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

    def instanceValHash(self):
        """TODO: need to implement this to depends on connection and 
            also table itself"""
        return 0

class SmvOutput(SmvIoModule):
    """Mixin which marks an SmvModule as one of the output & Reused as a base class
        for all Output modules

        Reuse the output marker mixin, should work for old mixin and new base class.
        Will make it symmetric to SmvInput when mixin usage is deprecated.

        As used for Mixin, SmvOutputs are distinct from other SmvModule in that
            * The -s and --run-app options of smv-run only run SmvOutputs and their dependencies.

        As used for base class, sub-class need to implement:
        
            - doRun

        Within doRun, assert_single_input should be called.

        User need to implement:

            - connectionName
            - tableName
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
        """The user-specified connection name

            Returns:
                (string)
        """
        # Implemented as None to be backword compatable as the mixin marker
        return None
    
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