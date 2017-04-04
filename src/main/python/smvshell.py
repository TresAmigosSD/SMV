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

from smv import SmvApp
from pyspark.sql import DataFrame

from smv import CsvAttributes

jvmShellCmd = SmvApp.getInstance()._jvm.org.tresamigos.smv.shell.ShellCmd

df = lambda name: SmvApp.getInstance().runModuleByName(name)
def ddf(fqn):
    print "ddf has been removed. df now runs modules dynamically. Use df instead of ddf."
def pdf(fqn):
    print "pdf has been removed. Run modules dynamically with df instead."

openHive = lambda tableName: DataFrame(jvmShellCmd.openHive(tableName), SmvApp.getInstance().sqlContext)
openCsv = lambda path: DataFrame(jvmShellCmd.openCsv(path), SmvApp.getInstance().sqlContext)

def help():
    import re
    strip_margin = lambda text: re.sub('\n[ \t]*\|', '\n', text)

    print strip_margin(
     """Here is a list of SMV-shell command
       |
       |Please refer to the API doc for details:
       |https://github.com/TresAmigosSD/SMV/blob/master/docs/user/run_shell.md
       |
       |  * lsStage()
       |  * ls()
       |  * ls(stageName)
       |  * lsDead()
       |  * lsDead(stageName)
       |  * lsDeadLead()
       |  * lsDeadLead(stageName)
       |  * graph()
       |  * graph(stageName)
       |  * ancestors(datasetName)
       |  * descendants(datasetName)
       |  * now()
       |  * discoverSchema(filePath)
       """
   )

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

def lsDeadLeaf(stageName = None):
    if(stageName is None):
        print(jvmShellCmd.lsDeadLeaf())
    else:
        print(jvmShellCmd.lsDeadLeaf(stageName))

def ancestors(dsname):
    print(jvmShellCmd.ancestors(dsname))

def descendants(dsname):
    print(jvmShellCmd.descendants(dsname))

def graph(stageName = None):
    if(stageName is None):
        print(jvmShellCmd._graph())
    else:
        print(jvmShellCmd._graph(stageName))

def graphStage():
    print(jvmShellCmd._graphStage())

def now():
    print(jvmShellCmd.now())

def discoverSchema(path, n=100000, ca=SmvApp.getInstance().defaultCsvWithHeader()):
   SmvApp.getInstance()._jvm.SmvPythonHelper.discoverSchema(path, n, ca)
