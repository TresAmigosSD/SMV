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

import smv
from smv.utils import smvhash
from smv.provider import SmvProvider

class SmvConnectionInfo(SmvProvider):
    """Base class for all IO connection info

        A connection is defined by a group of attributes, and those attributes
        are provided from smv props. For example:

            - smv.conn.myjdbc.class = smv.conn.SmvJdbcConnectionInfo
            - smv.conn.myjdbc.url = postgress://localhost:1000/...

        Args:
            name(str) connection name, in the example is "myjdbc"
            props(dict(str:str)) key-value pairs from smvconf.merged_props()
    """

    @staticmethod
    def provider_type():
        return "conn"

    @abc.abstractmethod
    def attributes():
        """a list of attributes as strings for the concrete connection type"""
        # Need to be @staticmethod when define in concrete class

    def get_contents(self, smvApp):
        """Return a list of file/table names
        """
        return []

    def __init__(self, name, props):
        self.name = name
        self._from_props(props)

    def _from_props(self, props):
        """Load SmvConnectionInfo attributes from smv props
            The props for connections all start with "smv.conn.{connection_name}.".
            Each connection must have a "smv.conn.{connection_name}.class" prop.
            For different connections, other props may exists for the attributes
        """
        prop_prefix = "smv.conn.{}.".format(self.name)
        for a in self.attributes():
            prop_key = prop_prefix + a
            setattr(self, a, props.get(prop_key, None))

    def conn_hash(self):
        res = 0
        for a in self.attributes():
            attr = getattr(self, a)
            if (attr is not None):
                res += smvhash(attr)

        return res
