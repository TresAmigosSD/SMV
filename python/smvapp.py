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
    # skip the first argument, which is this program
    smv = SmvApp.init(sys.argv[1:])
    app = smv.app

    print("----------------------------------------")
    print("will run the following modules:")
    mods = app.moduleNames()
    for name in mods:
        print("   " + name)
    print("----------------------------------------")

    for name in mods:
        smv.run_python_module(name)

    table = app.config().exportHive()
    if (table.isDefined()):
        app.verifyConfig()
        app.exportHive(smv.run_python_module(mods[0])._jdf, table.get())
