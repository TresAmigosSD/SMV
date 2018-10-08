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

from smv.utils import smv_copy_array, scala_seq_to_list

class DataSetMgr(object):
    """The Python representation of DataSetMgr.
    """

    def __init__(self, sc, j_dsm):
        self.sc = sc
        self._jvm = sc._jvm
        self.j_dsm = j_dsm

        from py4j.java_gateway import java_import
        java_import(self._jvm, "org.tresamigos.smv.python.SmvPythonHelper")
        java_import(self._jvm, "org.tresamigos.smv.DataSetRepoFactoryPython")

        self.helper = self._jvm.SmvPythonHelper

    def load(self, *urns):
        """Load SmvDataSets for specified URNs
        
        Args:
            *urns (str): list of URNs as strings

        Returns:
            list(SmvDataSet): list of Scala SmvDataSets (j_ds)
        """
        return self.helper.dsmLoad(self.j_dsm, smv_copy_array(self.sc, *urns))

    def inferDS(self, *partial_names):
        """Return DSs from a list of partial names

        Args:
            *partial_names (str): list of partial names

        Returns:
            list(SmvDataSet): list of Scala SmvDataSets (j_ds)
        """
        return self.helper.dsmInferDS(self.j_dsm, smv_copy_array(self.sc, *partial_names))

    def inferUrn(self, partial_name):
        """Return URN string from partial name
        """
        return self.inferDS(partial_name)[0].urn().toString()

    def register(self, repo_factory):
        """Register python repo factory
        """
        j_rfact = self._jvm.DataSetRepoFactoryPython(repo_factory)
        self.j_dsm.register(j_rfact)

    def allDataSets(self):
        """Return all the SmvDataSets in the app
        """
        j_dss = self.j_dsm.allDataSets()
        return scala_seq_to_list(self._jvm, j_dss)

    def modulesToRun(self, modPartialNames, stageNames, allMods):
        return self.helper.dsmModulesToRun(
            self.j_dsm,
            smv_copy_array(self.sc, *modPartialNames), 
            smv_copy_array(self.sc, *stageNames), 
            allMods
        )
