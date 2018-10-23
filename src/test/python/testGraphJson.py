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

import json

from test_support.smvbasetest import SmvBaseTest
import smv.smvappinfo
from smv import *

class SmvGraphJsonTest(SmvBaseTest):
    @classmethod
    def smvAppInitArgs(cls):
        return ['--smv-props', 'smv.stages=stage']

    def test_graph_json_docstr(self):
        j_str = self.smvApp.get_graph_json()
        j_obj = json.loads(j_str)
        res = j_obj['nodes'][0]['description']

        exp = """This is the test DS X's docstring
        It is multi lines.
        with "double" quotes and 'single' quote
    """
        assert (res == exp)

    def test_graph_fqns(self):
        j_str = self.smvApp.get_graph_json()
        j_obj = json.loads(j_str)
        n_fqns = [n['fqn'] for n in j_obj['nodes']]

        exp = ['stage.modules.X', 'stage.modules.Y']
        assert (sorted(n_fqns) == sorted(exp))

    def test_graph_edges(self):
        j_str = self.smvApp.get_graph_json()
        j_obj = json.loads(j_str)

        res = j_obj['edges'][0]
        exp = ['stage.modules.X', 'stage.modules.Y']
        assert (sorted(res) == sorted(exp))
    