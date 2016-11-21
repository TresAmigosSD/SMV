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
from smv import SmvApp

if __name__ == "__main__":
    import compileall
    r = compileall.compile_dir('src/main/python', quiet=1)
    if not r:
        exit(-1)

    # skip the first argument, which is this program
    SmvApp.init(sys.argv[1:])
    j_smv = SmvApp._jsmv

    print("----------------------------------------")
    print("will run the following modules:")
    mods = j_smv.moduleNames(SmvApp.repo)
    for name in mods:
        print("   " + name)
    print("----------------------------------------")

    publish = j_smv.publishVersion().isDefined()
    for name in mods:
        if publish:
            SmvApp.publishModule(name)
        else:
            SmvApp.runModule(name)

    # TODO: get table name from SmvPyOutput instead of hardcoding!!!
    if (j_smv.publishHive()):
        j_smv.exportDataFrameToHive(SmvApp.runModule(mods[0])._jdf, "0_replace_me")
