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
from smv.utils import infer_full_name_from_part


class SmvRunInfo(object):
    """collection of a module's running info with:
    
        - metadata
        - metahistory
    """
    def __init__(self, meta, metahist):
        self.metadata = meta
        self.metadata_history = metahist

class SmvRunInfoCollector(object):
    """A list of SmvRunInfos from a run transaction, and methods
        to help reporting on them
    """
    def __init__(self):
        self.runinfos = {}

    def add_runinfo(self, fqn, meta, meta_hist):
        self.runinfos.update({fqn: SmvRunInfo(meta, meta_hist)})

    def fqns(self):
        """Returns a list of FQNs for all datasets that ran"""
        return [fqn for fqn in self.runinfos]

    def _infer_fqn(self, ds_name):
        """ds_name for user to use could be partial name, infer full fqn from partial name"""
        return infer_full_name_from_part(self.fqns(), ds_name)

    def dqm_validation(self, ds_name):
        """Returns the DQM validation result for a given dataset

        Returns:
            A dictionary representation of the dqm validation result

        Raises:
            py4j.protocol.Py4JError: if there is java call error or
                there is no validation result for the specified dataset
                (e.g. caused by a typo in the name)

        """
        metadata = self.metadata(ds_name)
        if (not metadata):
            return {}
        return metadata["_dqmValidation"]

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
        """Returns the metadata for a given dataset as a dict
        """
        return self.runinfos.get(self._infer_fqn(ds_name)).metadata._metadata

    def metadata_history(self, ds_name):
        """Returns the metadata history for a given dataset as a list(dict)
        """
        return self.runinfos.get(self._infer_fqn(ds_name)).metadata_history._hist_list

    def show_report(self, ds_name=None, show_history=False):
        """Print detailed report of information collected

            Args:
                ds_name (str): report only of named ds if not None
                show_history (bool): include metadata history in report if True (default False)
        """
        if ds_name is None:
            fqns = self.fqns()
        else:
            fqns = [self._infer_fqn(ds_name)]
        msg = 'datasets: %s' % fqns

        def items_to_report(fqn):
            validation = self.dqm_validation(fqn)
            metadata = self.metadata(fqn)
            # Remove validation results from metadata (if they exist) as we are reporting it above
            try:
                del metadata['_dqmValidation']
            except:
                pass

            items = [("dqm validation", validation), ("metadata", metadata)]
            if show_history:
                history = self.metadata_history(fqn)
                items.append(("metadata history", history))

            return items

        for fqn_to_report in fqns:
            msg += '\n+ %s' % fqn_to_report
            for name, value in items_to_report(fqn_to_report):
                msg += '\n|- {}:'.format(name)
                msg += '\n     ' + pformat(value, indent=5)

        print(msg)
