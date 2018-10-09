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

from smv.utils import scala_seq_to_list

class SmvRunConfig(object):
    """DEPRECATED

        Run config accessor methods have been absorbed by SmvDataSet, so `SmvRunConfig` is maintained
        to support existing projects. `SmvRunConfig's` influence on the dataset hash is preserved so that
        modules do not have to transition overnight to using `SmvDataSet.requiresConfig` in order for the
        config to influence the dataset hash.
    """

    def _all_run_conf_keys(self):
        """Return all possible run conf keys"""
        return self.smvApp.py_smvconf.get_run_config_keys()