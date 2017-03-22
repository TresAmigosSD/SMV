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

from smv import smvPy
from pyspark.sql import DataFrame

from smv import CsvAttributes

jvmShellCmd = smvPy._jvm.org.tresamigos.smv.shell.ShellCmd

df = lambda name: smvPy.runModuleByName(name)
def ddf(fqn):
    print "ddf has been removed. df now runs modules dynamically. Use df instead of ddf."
def pdf(fqn):
    print "pdf has been removed. Run modules dynamically with df instead."

openHive = lambda tableName: DataFrame(jvmShellCmd.openHive(tableName), smvPy.sqlContext)
openCsv = lambda path: DataFrame(jvmShellCmd.openCsv(path), smvPy.sqlContext)

def lsStage():
    print(jvmShellCmd.lsStage())

def ls(stageName = None):
    if(stageName is None):
        print(jvmShellCmd.ls())
    else:
        print(jvmShellCmd.ls(stageName))

def lsDead(stageName = None):
    if(stageName is None):
        print(jvmShellCmd.lsDead())
    else:
        print(jvmShellCmd.lsDead(stageName))

def lsLeaf(stageName = None):
    if(stageName is None):
        print(jvmShellCmd.lsLeaf())
    else:
        print(jvmShellCmd.lsLeaf(stageName))

def descendants(urn):
    print(jvmShellCmd.descendants("mod:"+urn))

def graph(stageName = None):
    if(stageName is None):
        print(jvmShellCmd._graph())
    else:
        print(jvmShellCmd._graph(stageName))

def now():
    print(jvmShellCmd.now())

def discoverSchema(path, n=100000, ca=smvPy.defaultCsvWithHeader()):
    smvPy._jvm.SmvPythonHelper.discoverSchema(path, n, ca)
