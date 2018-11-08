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
from smvgenericmodule import SmvGenericModule
from smviostrategy import SmvNonOpIoStrategy


class SmvIoModule(SmvGenericModule):

    def isEphemeral(self):
        """SmvIoModules are always ephemeral"""
        return True

    def persistStrategy(self):
        return SmvNonOpIoStrategy()

    @abc.abstractmethod
    def connection_name(self):
        """Name of the connection to read/write"""
    
    @abc.abstractmethod
    def get_connection(self, name):
        """Get connection instance from name"""
