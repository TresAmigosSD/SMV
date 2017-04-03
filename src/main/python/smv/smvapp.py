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

import sys, os

from pyspark import SparkContext
from pyspark.sql import HiveContext
from smv import smvPy
from smv.error import SmvRuntimeError
import compileall

# TODO: need to decouple the smvPy.init from the .run() method.  smv server only needs to do init.
# TODO: add_source/run should be instance methods.  Now, they are just global functions.
# TODO: should add src/main/python by default in the consturctor of SmvApp.
class SmvApp(object):
    @classmethod
    def prepend_source(cls,d):
        # Need to move `src/main/python` to the front of the sys.path
        codePath = os.path.abspath(d)
        sys.path.insert(1, codePath)

    @classmethod
    def run(self):
        # skip the first argument, which is this program
        smvPy.init(sys.argv[1:])
        smvPy.j_smvApp.run()
