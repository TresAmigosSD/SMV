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

from test_support.smvbasetest import SmvBaseTest
from smv import *
from smv.error import SmvDqmValidationError, SmvRuntimeError
from smv.smvdataset import ModulesVisitor

from pyspark.sql import DataFrame

class SmvFrameworkTest2(SmvBaseTest):
    @classmethod
    def smvAppInitArgs(cls):
        return ['--smv-props', 'smv.stages=stage']

    def test_visit_queue(self):
        fqns = ["stage.modules.M3", "stage.modules.M2"]
        ds = self.load2(*fqns)
        
        queue =  ModulesVisitor(ds).queue

        names = [m.fqn()[14:] for m in queue]
        self.assertEqual(names, ['I1', 'M1', 'M2', 'M3'])