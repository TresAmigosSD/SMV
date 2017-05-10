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

def df(name, forceRun = False):
    """The DataFrame result of running the named module

        Args:
            name (str): The name of a module. Does not have to be the FQN.
            forceRun (bool): True if the module should be forced to run even if it has persisted output. False otherwise.

        Returns:
            (DataFrame): The result of running the named module.
    """
    return SmvApp.getInstance().runModuleByName(name, forceRun)

def openHive(tableName):
    """Read in a Hive table as a DataFrame

        Args:
            tableName (str): The name of the Hive table

        Returns:
            (DataFrame): The resulting DataFrame
    """
    return DataFrame(jvmShellCmd.openHive(tableName), SmvApp.getInstance().sqlContext)

def openCsv(path):
    """Read in a CSV file as a DataFrame

        Args:
            path (str): The path of the CSV file

        Returns:
            (DataFrame): The resulting DataFrame
    """
    return DataFrame(jvmShellCmd.openCsv(path), SmvApp.getInstance().sqlContext)

def help():
    """Print a list of the SMV helper functions available in the shell
    """
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
       |  * lsDeadLeaf()
       |  * lsDeadLeaf(stageName)
       |  * graph()
       |  * graph(stageName)
       |  * ancestors(datasetName)
       |  * descendants(datasetName)
       |  * now()
       |  * discoverSchema(filePath)
       """
   )

def lsStage():
    """List all the stages
    """
    print(jvmShellCmd.lsStage())

def ls(stageName = None):
    """List all datasets in a stage

        Args:
            stageName (str): The name of the stage. Defaults to None, in which ase all datasets in all stages will be listed.
    """
    if(stageName is None):
        print(jvmShellCmd.ls())
    else:
        print(jvmShellCmd.ls(stageName))

def lsDead(stageName = None):
    """List dead datasets in a stage

        Args:
            stageName (str): The name of the stage. Defaults to None, in which ase all datasets in all stages will be listed.
    """
    if(stageName is None):
        print(jvmShellCmd.lsDead())
    else:
        print(jvmShellCmd.lsDead(stageName))

def lsDeadLeaf(stageName = None):
    """List 'deadLeaf' datasets in a stage

        A 'deadLeaf' dataset is dataset for which "no modules in the stage depend
        on it, excluding Output modules"

        Note: a `deadLeaf` dataset must be `dead`, but some `dead` datasets aren't
        `leaves`.

        Args:
            stageName (str): The name of the stage. Defaults to None, in which ase all datasets in all stages will be listed.
    """
    if(stageName is None):
        print(jvmShellCmd.lsDeadLeaf())
    else:
        print(jvmShellCmd.lsDeadLeaf(stageName))

def ancestors(dsname):
    """List all ancestors of a dataset

        Ancestors of a dataset are the dataset it depends on, directly or
        in-directly, including datasets from other stages.

        Args:
            dsname (str): The name of an SmvDataSet
    """
    print(jvmShellCmd.ancestors(dsname))

def descendants(dsname):
    """List all descendants of a dataset

        Descendants of a dataset are the datasets which depend on it directly or
        in-directly, including datasets from other stages

        Args:
            dsname (str): The name of an SmvDataSet
    """
    print(jvmShellCmd.descendants(dsname))

def graph(stageName = None):
    """Print ascii graph of all datasets in a given stage or all stages

        Args:
            dsname (str): The name of an SmvDataSet
    """
    if(stageName is None):
        print(jvmShellCmd._graph())
    else:
        print(jvmShellCmd._graph(stageName))

def graphStage():
    """Print ascii graph of all stages (not datasets)
    """
    print(jvmShellCmd._graphStage())

def now():
    """Print current time
    """
    print(jvmShellCmd.now())

def discoverSchema(path, n=100000, ca=SmvApp.getInstance().defaultCsvWithHeader()):
    """Try best to discover Schema from raw Csv file

        Will save a schema file with postfix ".toBeReviewed" in local directory.

        Args:
            path (str): Path to te CSV file
            n (int): Number of records to check for schema discovery, default 100k
            ca (CsvAttributes): Defaults to CsvWithHeader
    """
    SmvApp.getInstance()._jvm.SmvPythonHelper.discoverSchema(path, n, ca)
