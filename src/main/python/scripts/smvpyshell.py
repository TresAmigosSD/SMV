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

"""
Initializes Smv in Pyspark Shell
"""

# Add auto-completion and a stored history file of commands to your Python
# interactive interpreter. Requires Python 2.0+, readline. Autocomplete is
# bound to the Esc key by default (you can change it - see readline docs).

import atexit
import os

try:
    import readline
except ImportError:
    import pyreadline as readline
import rlcompleter

from smv import SmvApp

historyPath = os.path.expanduser("~/.pysparkhistory")

def save_history(historyPath=historyPath):
    import readline
    readline.write_history_file(historyPath)

if os.path.exists(historyPath):
    try:
        readline.read_history_file(historyPath)
    except:
        os.remove(historyPath)
        print("Unable to read history, deleting history file.")

atexit.register(save_history)

# Import commonly used pyspark lib
import pyspark.sql.functions as F
import pyspark.sql.types as T

sc.setLogLevel("ERROR")

with open(".smv_shell_all_args") as fp:
    args = fp.readline()

user_args = args.split()
app = SmvApp.createInstance(user_args, spark)

from smv.smvshell import *

# Import user-defined helpers
app.prepend_source("conf/")
if os.path.exists("conf/smv_shell_app_init.py"):
  from smv_shell_app_init import *

del os, atexit, readline, rlcompleter, save_history, historyPath
