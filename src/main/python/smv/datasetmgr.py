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
import smv

from smv.utils import smv_copy_array, scala_seq_to_list, list_distinct, infer_full_name_from_part
from smv.datasetresolver import DataSetResolver

class DataSetMgr(object):
    """The Python representation of DataSetMgr.
    """

    def __init__(self, _jvm, smvconfig):
        self._jvm = _jvm

        self.smvconfig = smvconfig
        self.dsRepoFactories = []

        from py4j.java_gateway import java_import
        java_import(self._jvm, "org.tresamigos.smv.python.SmvPythonHelper")
        java_import(self._jvm, "org.tresamigos.smv.DataSetRepoFactoryPython")

        self.helper = self._jvm.SmvPythonHelper

    def stages(self):
        return self.smvconfig.stage_names()

    def tx(self):
        """Create a TXContext for multiple places, avoid the long TXContext line
        """
        return TXContext(self._jvm, self.dsRepoFactories, self.stages())


    def load(self, *fqns):
        """Load SmvGenericModules for specified FQNs

        Args:
            *fqns (str): list of FQNs as strings

        Returns:
            list(SmvGenericModules): list of Scala SmvGenericModules (j_ds)
        """
        with self.tx() as tx:
            return tx.load(fqns)

    def inferDS(self, *partial_names):
        """Return DSs from a list of partial names

        Args:
            *partial_names (str): list of partial names

        Returns:
            list(SmvGenericModules): list of SmvGenericModules
        """
        with self.tx() as tx:
            return tx.inferDS(partial_names)

    def inferFqn(self, partial_name):
        """Return FQN string from partial name
        """
        with self.tx() as tx:
            return tx._inferFqn([partial_name])[0]

    def register(self, repo_factory):
        """Register python repo factory
        """
        self.dsRepoFactories.append(repo_factory)

    def allDataSets(self):
        """Return all the SmvGenericModules in the app
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

class TXContext(object):
    """Create a TX context for "with tx() as tx" syntax
    """
    def __init__(self, _jvm, resourceFactories, stages):
        self._jvm = _jvm
        self.resourceFactories = resourceFactories
        self.stages = stages

    def __enter__(self):
        return TX(self._jvm, self.resourceFactories, self.stages)

    def __exit__(self, type, value, traceback):
        pass


class TX(object):
    """Abstraction of the transaction boundary for loading SmvGenericModules.
        A TX object

        * will instantiate a set of repos when itself instantiated and will
        * reuse the same repos for all queries. This means that each new TX object will
        * reload the SmvGenericModules from source **once** during its lifetime.

        NOTE: Once a new TX is created, the well-formedness of the SmvGenericModules provided
        by the previous TX is not guaranteed. Particularly it may become impossible
        to run modules from the previous TX.
    """
    def __init__(self, _jvm, resourceFactories, stages):
        self.repos = [rf.createRepo() for rf in resourceFactories]
        self.stages = stages
        self.resolver = DataSetResolver(self.repos[0])
        self.log = smv.logger

    def load(self, fqns):
        return self.resolver.loadDataSet(fqns)

    def inferDS(self, partial_names):
        return self.load(self._inferFqn(partial_names))

    def allDataSets(self):
        return self.load(self._allFqns())

    def allOutputModules(self):
        return self._filterOutput(self.allDataSets())

    def outputModulesForStage(self, stageNames):
        return self._filterOutput(self._dsForStage(stageNames))

    def _dsForStage(self, stageNames):
        return self.load(self._fqnsForStage(stageNames))

    def _fqnsForStage(self, stageNames):
        return [u
            for repo in self.repos
            for s in stageNames
            for u in repo.dataSetsForStage(s)
        ]

    def _allFqns(self):
        if (len(self.stages) == 0):
            log.warn("No stage names configured. Unable to discover any modules.")
        return self._fqnsForStage(self.stages)

    def _inferFqn(self, partial_names):
        def fqn_str(pn):
            return infer_full_name_from_part(
                self._allFqns(),
                pn
            )

        return [fqn_str(pn) for pn in partial_names]

    def _filterOutput(self, dss):
        return [ds for ds in dss if ds.isSmvOutput()]
