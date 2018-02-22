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
        coll.ds_names  # returns
    """
    def __init__(self, jcollector):
        self.jcollector = jcollector

    def fqns(self):
        """Returns a list of FQNs for all datasets that ran"""
        return self.jcollector.dsFqnsAsJava()

    def dqm_validation(self, ds_name):
        """Returns the DQM validation result for a given dataset

        Returns:
            A dictionary representation of the dqm validation result

        Raises:
            py4j.protocol.Py4JError: if there is java call error or
                there is no validation result for the specified dataset
                (e.g. caused by a typo in the name)

        """
        java_result = self.jcollector.getDqmValidationResult(ds_name)
        if java_result is None:
            return {}
        return json.loads(java_result.toJSON())

    def dqm_state(self, ds_name):
        """Returns the DQM state for a given dataset

        Returns:
            A dictionary representation of the dqm state

        Raises:
            py4j.protocol.Py4JError: if there is java call error or
                there is no validation result or dqm state for the
                specified dataset (e.g. caused by a typo in the name)

        """
        validation = self.dqm_validation(ds_name)
        if 'dqmStateSnapshot' in validation:
            return validation['dqmStateSnapshot']
        return {}

    def metadata(self, ds_name):
        """Returns the metadata for a given dataset

        Returns:
            A dictionary representation of the metadata

        Raises:
            py4j.protocol.Py4JError: if there is java call error or
                there is no metadata for the specified dataset
                (e.g. caused by a typo in the name)

        """
        java_result = self.jcollector.getMetadata(ds_name)
        if java_result is None:
            return {}
        return json.loads(java_result.toJson())

    def metadata_history(self, ds_name):
        """Returns the metadata history for a given dataset

        Returns:
            A list of metadata dictionaries in reverse chronological order

        Raises:
            py4j.protocol.Py4JError: if there is java call error or
                there is no metadata for the specified dataset
                (e.g. caused by a typo in the name)

        """
        java_result = self.jcollector.getMetadataHistory(ds_name)
        if java_result is None:
            return {}
        # note that the json is an object with the structure
        # {
        #   "history": [
        #     {...},
        #     ...
        #   ]
        # }
        return json.loads(java_result.toJson())['history']

    def show_report(self, ds_name=None, show_history=False):
        if ds_name is None:
            fqns = self.fqns()
        else:
            fqns = [self.jcollector.inferFqn(ds_name)]
        msg = 'datasets: %s' % fqns
        for fqn_to_report in fqns:
            msg += '\n+ %s' % fqn_to_report
            msg += '\n|- dqm validation:'
            msg += '\n     ' + pformat(self.dqm_validation(fqn_to_report), indent=5)
            msg += '\n|- metadata:'
            msg += '\n     ' + pformat(self.metadata(fqn_to_report), indent=5)
            if show_history:
                msg += '\n|- metadata history:'
                msg += '\n     ' + pformat(self.metadata_history(fqn_to_report), indent=5)
        print(msg)
