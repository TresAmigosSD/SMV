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
        return json.loads(java_result.toJSON())

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
        return json.loads(java_result.toJson())

    def metadata_history(self, dsFqn):
        """Returns the metadata for a given dataset

        Returns:
            A dictionary representation of the metadata

        Raises:
            py4j.protocol.Py4JError: if there is java call error or
                there is no metadata for the specified dataset
                (e.g. caused by a typo in the name)

        """
        java_result = self.jcollector.getMetadataHistory(dsFqn)
        return json.loads(java_result.toJson())
