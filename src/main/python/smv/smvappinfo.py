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

"""Provides SmvApp module list, dependency graph etc.
"""
from utils import scala_seq_to_list
import json

class SmvAppInfo(object):
    def __init__(self, smvApp):
        self.smvApp = smvApp
        self.dsm = smvApp.dsm

    def _graph(self):
        """Build graph with nodes:list(SmvDataSet) and edges:list((SmvDataSet, SmvDataSet))
        """
        nodes = self.dsm.allDataSets()
        edges = []
        for ds in nodes:
            from_ds = scala_seq_to_list(self.smvApp._jvm, ds.resolvedRequiresDS())
            edges.extend([(n, ds) for n in from_ds if n in nodes])
        
        return (nodes, edges)

    def get_graph_json(self):
        """Create dependency graph Json string
        """
        (nodes, edges) = self._graph()
        def node_type(n):
            t = n.dsType()
            if (t == "Input"):
                return "file"
            else:
                return t[:1].lower() + t[1:]

        def node_dict(n):
            return {
                "fqn": n.fqn(),
                "type": node_type(n),
                "version": n.version(),
                "needsToRun": n.needsToRun(),
                "description": n.description()
            }

        def edge_pair(f, t):
            return [f.fqn(), t.fqn()]

        return json.dumps({
            "nodes": [node_dict(n) for n in nodes],
            "edges": [edge_pair(p[0], p[1]) for p in edges]
        })
