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

"""Provides dependency graphing of SMV modules.
"""
from utils import smv_copy_array

class SmvDependencyGraph(object):
    def __init__(self, smvPy, stageNames = None):
        self.smvPy = smvPy
        self.stageNames = smvPy.j_smvApp.stages() if stageNames is None else smv_copy_array(smvPy.sc, stageNames)

    def __repr__(self):
        # use side effect to show graph, as it is a unicode string and
        # can't be displayed via __repr__
        print(self.smvPy.j_smvPyClient.asciiGraph())
        return ''

    def _repr_png_(self):
        return bytes(self.smvPy.j_smvPyClient.graph(self.smvPy.j_smvApp.stages(), 'png'))
