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
import readline
import rlcompleter

historyPath = os.path.expanduser("~/.pysparkhistory")

def save_history(historyPath=historyPath):
    import readline
    readline.write_history_file(historyPath)

if os.path.exists(historyPath):
    readline.read_history_file(historyPath)

atexit.register(save_history)
del os, atexit, readline, rlcompleter, save_history, historyPath

# Get a random port for callback server
import random;
callback_server_port = random.randint(20000, 65535)

# Import commonly used pyspark lib
from pyspark.sql.functions import *
from pyspark.sql.types import *

sc.setLogLevel("ERROR")

# Imnport smv
from smv import smvPy
app = smvPy.init(['-m', 'None', '--cbs-port', str(callback_server_port)], spark)

from smvshell import *

def discoverSchema(path, n=100000, ca=smvPy.defaultCsvWithHeader()):
    smvPy._jvm.SmvPythonHelper.discoverSchema(path, n, ca)

# The following code evokes a loop of importing
# The ./bin/pyspark script stores the old PYTHONSTARTUP value in OLD_PYTHONSTARTUP,
# which allows us to execute the user's PYTHONSTARTUP file:
#_pythonstartup = os.environ.get('OLD_PYTHONSTARTUP')
#if _pythonstartup and os.path.isfile(_pythonstartup):
#    with open(_pythonstartup) as f:
#        code = compile(f.read(), _pythonstartup, 'exec')
#        exec(code)
