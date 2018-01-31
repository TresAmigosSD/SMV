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

"""Easy Python access to SmvRunInfoCollector and related Scala classes.

Todo:
    * document example use
"""
import json
from pprint import pformat


class SmvRunInfoCollector(object):
    """Python wrapper to its counterpart on the Scala side

    Example:
        df, coll = smvApp.runModule(...)
        coll.dsFqns  # returns
    """
    def __init__(self, jcollector):
        self.jcollector = jcollector

    def fqns(self):
        """Returns a list of FQNs for all datasets that ran"""
        return self.jcollector.dsFqnsAsJava()

    def dqm_validation(self, dsFqn):
        """Returns the DQM validation result for a given dataset

        Returns:
            A dictionary representation of the dqm validation result

        Raises:
            py4j.protocol.Py4JError: if there is java call error or
                there is no validation result for the specified dataset
                (e.g. caused by a typo in the name)

        """
        java_result = self.jcollector.getDqmValidationResult(dsFqn)
        if java_result is None:
            return {}
        return json.loads(java_result.toJSON())

    def dqm_state(self, dsFqn):
        """Returns the DQM state for a given dataset

        Returns:
            A dictionary representation of the dqm state

        Raises:
            py4j.protocol.Py4JError: if there is java call error or
                there is no validation result or dqm state for the
                specified dataset (e.g. caused by a typo in the name)

        """
        validation = self.dqm_validation(dsFqn)
        if 'dqmStateSnapshot' in validation:
            return validation['dqmStateSnapshot']
        return {}

    def metadata(self, dsFqn):
        """Returns the metadata for a given dataset

        Returns:
            A dictionary representation of the metadata

        Raises:
            py4j.protocol.Py4JError: if there is java call error or
                there is no metadata for the specified dataset
                (e.g. caused by a typo in the name)

        """
        java_result = self.jcollector.getMetadata(dsFqn)
        if java_result is None:
            return {}
        return json.loads(java_result.toJson())

    def metadata_history(self, dsFqn):
        """Returns the metadata history for a given dataset

        Returns:
            A list of metadata dictionaries in reverse chronological order

        Raises:
            py4j.protocol.Py4JError: if there is java call error or
                there is no metadata for the specified dataset
                (e.g. caused by a typo in the name)

        """
        java_result = self.jcollector.getMetadataHistory(dsFqn)
        if java_result is None:
            return {}
        return json.loads(java_result.toJson())

    def show_report(self):
        msg = 'datasets: %s' % self.fqns()
        for fqn in self.fqns():
            msg += '\n+ %s' % fqn
            msg += '\n|- dqm validation:'
            msg += '\n     ' + pformat(self.dqm_validation(fqn), indent=5)
            msg += '\n|- metadata:'
            msg += '\n     ' + pformat(self.metadata(fqn), indent=5)
            msg += '\n|- metadata history:'
            msg += '\n     ' + pformat(self.metadata_history(fqn), indent=5)
        print(msg)
