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
        self.stages = smvApp.stages()

    def _graph(self):
        """Build graph with nodes:list(SmvDataSet) and edges:list((SmvDataSet, SmvDataSet))
        """
        nodes = self.dsm.allDataSets()
        edges = []
        for ds in nodes:
            from_ds = scala_seq_to_list(self.smvApp._jvm, ds.resolvedRequiresDS())
            edges.extend([(n, ds) for n in from_ds if n in nodes])
        
        return (nodes, edges)

    def _common_prefix(self, str_list):
        """Given a list of strings, return the longest common prefix (LCP)
        """
        if not str_list: return ''

        # The algorithm depends on the ordering of strings, so LCP
        # of min and max in the group is the LCP of the entire group
        s1 = min(str_list)
        s2 = max(str_list)
        for i, c in enumerate(s1):
            if c != s2[i]:
                return s1[:i]
        return s1

    def _base_name(self, ds):
        """Return DS's fqn with common prefix removed
        """
        shared_prefix = self._common_prefix(self.stages) + "."
        fqn = ds.fqn()
        if (fqn.startswith(shared_prefix)):
            return fqn[len(shared_prefix):]
        else:
            return fqn

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

    def create_dot_graph(self):
        """Create graphviz dot graph string for the whole app
        """
        (nodes, edges) = self._graph()

        clusters = {}
        for s in self.stages:
            n_in_s = [n for n in nodes if n.parentStage().getOrElse(None) == s]
            clusters.update({s: n_in_s})

        def _node_str(ds): 
            if (ds.dsType == "Input"):
                return '  "{}" [shape=box, color="pink"]'.format(self._base_name(ds))
            else:
                return '  "{}"'.format(self._base_name(ds))
        def _link_str(f, t):
            return '  "{}" -> "{}" '.format(self._base_name(f), self._base_name(t))

        def _cluster_str(i, s, ns):
            return '  subgraph cluster_{} {{\n'.format(i) \
                + '    label="{}"\n'.format(s) \
                + '    color="#e0e0e0"\n'\
                + '    {}\n'.format("; ".join(['"{}"'.format(self._base_name(n)) for n in ns]))\
                + '  }'

        all_nodes = "\n".join([
            _node_str(n) for n in nodes
        ])

        all_links = "\n".join([
            _link_str(p[0], p[1]) for p in edges
        ])

        all_clusters = "\n".join([
            _cluster_str(i, s, ns) 
            for i, (s, ns) in enumerate(clusters.items())
        ])

        return 'digraph G {\n' \
            + '  rankdir="LR";\n'\
            + '  node [style=filled,color="lightblue"]\n'\
            + '{}\n{}\n{}\n'.format(all_nodes, all_clusters, all_links)\
            + '}'

