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


class SmvConnectionInfo(object):
    """Base class for all IO connection info

        A connection is defined by a group of attributes, and those attributes 
        are provided from smv props. For example:

            - smv.con.myjdbc.type = jdbc
            - smv.con.myjdbc.url = postgress://localhost:1000/...

        Args:
            name(str) connection name, in the example is "myjdbc"
            props(dict(str:str)) key-value pairs from smvconf.merged_props()
    """
    @abc.abstractmethod
    def attributes(self):
        """a list of attributes as strings for the concrete connection type"""

    @abc.abstractmethod
    def connection_type(cls):
        """abstract class method to specify connection type as a string"""

    def __init__(self, name, props):
        self.name = name
        self._from_props(props)

    def _from_props(self, props):
        """Create an SmvConnectionInfo instance from smv props
            The props for connections all start with "smv.con.{connection_name}.".
            Each connection must have a "smv.con.{connection_name}.type" prop.
            For different connections, other props may exists for the attributes
        """
        prop_prefix = "smv.con.{}.".format(self.name)
        for a in self.attributes():
            prop_key = prop_prefix + a
            setattr(self, a, props.get(prop_key, None))


class SmvJdbcConnectionInfo(SmvConnectionInfo):
    @classmethod
    def connection_type(cls):
        return "jdbc"

    def attributes(self):
        return ["url", "driver", "user", "password"]


_all_connections = [
    SmvJdbcConnectionInfo,
]

def getConnection(con_type):
    """Get an instance of SmvConnectionInfo from a con_type as a string
    """
    con_map = {c.connection_type(): c for c in _all_connections}
    if (con_type in con_map):
        return con_map.get(con_type)
    else:
        raise SmvRuntimeError("Connection type {} is not implemented".format(con_type))