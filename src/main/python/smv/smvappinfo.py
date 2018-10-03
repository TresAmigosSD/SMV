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

    def create_graph_json(self):
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

    def create_graph_dot(self):
        """Create graphviz dot graph string for the whole app
        """
        (nodes, edges) = self._graph()

        clusters = {}
        for s in self.stages:
            n_in_s = [n for n in nodes if n.parentStage().getOrElse(None) == s]
            clusters.update({s: n_in_s})

        def _node_str(ds): 
            if (ds.dsType() == "Input"):
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

    def _ds_ancestors(self, m):
        """return the module's ancestors as python list"""
        return scala_seq_to_list(self.smvApp._jvm, m.ancestors())

    def _dead_nodes(self):
        """return all the dead nodes"""
        nodes = self.dsm.allDataSets()
        outputs = [n for n in nodes if n.dsType() == "Output"]
        in_flow = set([n for o in outputs for n in self._ds_ancestors(o)])
        return [n for n in nodes if n not in in_flow and n not in outputs]

    def _list_dataset(self, dss, withprefix=False):
        """list ds names, return: list(str)"""
        prefix_map = {
            "Output": "O",
            "Input" : "I",
            "Module": "M",
            "Model":  "m",
            "ModelExec": "x"
        }

        def _node_str(n):
            t = n.dsType()
            bn = self._base_name(n)
            if(withprefix):
                return '({}) {}'.format(prefix_map[t], bn)
            else:
                return bn

        return [_node_str(n) for n in dss]

    def ls_stage(self):
        """list all stage names"""
        "\n".join(self.stages)

    def _ls_in_stage(self, s, nodes, indentation=""):
        """list modules in a stage"""
        out_list = self._list_dataset([
            n for n in nodes if n.parentStage().getOrElse(None) == s
        ], True)
        return "\n".join([indentation + ns for ns in out_list])

    def _ls(self, stage, nodes):
        """For given nodes, list the node names under their stages"""
        if(stage is None):
            return "\n".join([
                "{}:\n".format(s) + self._ls_in_stage(s, nodes, "  ") + "\n" 
                for s in self.stages
            ])
        else:
            return self._ls_in_stage(stage, nodes)

    def ls(self, stage=None):
        """List all modules, under their stages"""
        return self._ls(stage, self.dsm.allDataSets())

    def ls_dead(self, stage=None):
        """List all dead modules, under their stages"""
        return self._ls(stage, self._dead_nodes())

    def ls_ancestors(self, mname):
        """List given module's ancestors, under their stages"""
        m = self.dsm.inferDS(mname)[0]
        return self._ls(None, self._ds_ancestors(m))

    def ls_descendants(self, mname):
        """List given module's descendants, under their stages"""
        m = self.dsm.inferDS(mname)[0]
        nodes = self.dsm.allDataSets()
        descendants = [n for n in nodes if m.fqn() in [a.fqn() for a in self._ds_ancestors(n)]]
        return self._ls(None, descendants)
