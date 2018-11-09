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
    @abc.abstractmethod
    def attributes(self):
        """a list of attributes as strings"""

    def __init__(self, name, props):
        self.name = name
        self._from_props(props)

    def _from_props(self, props):
        prop_prefix = "smv.con.{}.".format(self.name)
        for a in self.attributes():
            prop_key = prop_prefix + a
            setattr(self, a, props.get(prop_key, None))


class SmvJdbcConnectionInfo(SmvConnectionInfo):
    def attributes(self):
        return ["url", "driver", "user", "password"]