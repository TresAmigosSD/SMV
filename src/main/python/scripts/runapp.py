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

from smv import SmvApp

# skip the first argument, which is this program
args = sys.argv[1:]

def parse_args(args):
    for idx,arg in enumerate(args):
        if arg.endswith(".py"):
            driver_script = arg
            driver_args = args[idx+1:]
            smv_args = args[:idx]
            return (driver_script, driver_args, smv_args)
    return (None, None, args)

driver_script, driver_args, smv_args = parse_args(args)

app = SmvApp.createInstance(smv_args)

if driver_script is None:
    app.run()
else:
    execfile(driver_script)
