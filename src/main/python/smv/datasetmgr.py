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

"""DataSetMgr entry class
This module provides the python entry point to DataSetMgr on scala side
"""

from smv.utils import smv_copy_array, scala_seq_to_list, list_distinct

class DataSetMgr(object):
    """The Python representation of DataSetMgr.
    """

    def __init__(self, sc, stages):
        self.sc = sc
        self._jvm = sc._jvm

        self.stages = stages
        self.dsRepoFactories = []

        from py4j.java_gateway import java_import
        java_import(self._jvm, "org.tresamigos.smv.python.SmvPythonHelper")
        java_import(self._jvm, "org.tresamigos.smv.DataSetRepoFactoryPython")

        self.helper = self._jvm.SmvPythonHelper

    def tx(self):
        return TX(self._jvm, self.dsRepoFactories, self.stages)

    def load(self, *urns):
        """Load SmvDataSets for specified URNs
        
        Args:
            *urns (str): list of URNs as strings

        Returns:
            list(SmvDataSet): list of Scala SmvDataSets (j_ds)
        """
        with self.tx() as tx:
            return tx.load(urns)

    def inferDS(self, *partial_names):
        """Return DSs from a list of partial names

        Args:
            *partial_names (str): list of partial names

        Returns:
            list(SmvDataSet): list of Scala SmvDataSets (j_ds)
        """
        with self.tx() as tx:
            return tx.inferDS(partial_names)

    def inferUrn(self, partial_name):
        """Return URN string from partial name
        """
        return self.inferDS(partial_name)[0].urn().toString()

    def register(self, repo_factory):
        """Register python repo factory
        """
        j_rfact = self._jvm.DataSetRepoFactoryPython(repo_factory)
        self.dsRepoFactories.append(j_rfact)

    def allDataSets(self):
        """Return all the SmvDataSets in the app
        """
        with self.tx() as tx:
            return tx.allDataSets()

    def modulesToRun(self, modPartialNames, stageNames, allMods):
        """Return a modules need to run
            Combine specified modules, (-m), stages, (-s) and if
            (--run-app) specified, all output modules
        """
        with self.tx() as tx:
            named_mods = tx.inferDS(modPartialNames)
            stage_mods = tx.outputModulesForStage(stageNames)
            app_mods = tx.allOutputModules() if allMods else []
            res = []
            res.extend(named_mods)
            res.extend(stage_mods)
            res.extend(app_mods)
            # Need to perserve the ordering
            return list_distinct(res)

class TX(object):
    def __init__(self, _jvm, resourceFactories, stages):
        self._jvm = _jvm
        self.resourceFactories = resourceFactories
        self.stages = stages

    def __enter__(self):
        return self._jvm.org.tresamigos.smv.python.SmvPythonHelper.createTX(
            self.resourceFactories, self.stages
        )

    def __exit__(self, type, value, traceback):
        pass

