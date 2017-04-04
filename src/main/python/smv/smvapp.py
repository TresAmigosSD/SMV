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
class SmvApp(object):
    def __init__(self, *sources):
        self.prepend_source("src/main/python")

    def prepend_source(self,source_dir):
        # Source must be added to front of path to make sure it is found first
        codePath = os.path.abspath(source_dir)
        sys.path.insert(1, codePath)

    # There may already be a SparkContext when SmvApp is initialized
    def init(self, smv_args, sc = None, sqlContext = None):
        smvPy.init(smv_args, sc, sqlContext)

    def run(self, smv_args):
        self.init(smv_args)
        smvPy.j_smvApp.run()
