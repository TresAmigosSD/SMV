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

import sys

from pyspark import SparkContext
from pyspark.sql import HiveContext
from smv import smvPy

if __name__ == "__main__":
    import compileall
    r = compileall.compile_dir('src/main/python', quiet=1)
    if not r:
        exit(-1)

    # skip the first argument, which is this program
    smvPy.init(sys.argv[1:])
    smvPy.j_smvApp.run()

    # TODO: The following should be removed when Scala side can
    # handle publish-hive SmvPyOutput tables
    j_smv = smvPy.j_smvPyClient

    mods = j_smv.moduleNames()

    for name in mods:
        if j_smv.publishHive():
            smvPy.publishHiveModule(name)
