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
"""SMV User Run Configuration Parameters

This module defined the SmvRunConfig class which can be mixed-in into an
SmvModule to get user configuration parameters at run-time.
"""

from smvapp import SmvApp
import traceback


class SmvRunConfig(object):
    """Mix-in class to SmvModules that enable the module to access user run
       configuration parameters at run time.
    """

    def smvGetRunConfig(self, key):
        """return the current user run configuration value for the given key."""
        return SmvApp.getInstance().j_smvPyClient.getRunConfig(key)

    def smvGetRunConfigAsInt(self, key):
        return int(self.getRunConfig(key))

    def smvGetRunConfigAsBool(self, key):
        sval = self.getRunConfig(key).strip().lower()
        return (sval == "1" or sval == "true")

    def _smvGetRunConfigHash(self):
        """return the app level hash of the all the current user config values"""
        return SmvApp.getInstance().j_smvPyClient.getRunConfigHash()
