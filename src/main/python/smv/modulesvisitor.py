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
from collections import OrderedDict

class ModulesVisitor(object):
    """Provides way to do depth and breadth first visit to the sub-graph
        of modules given a set of roots
    """
    def __init__(self, roots):
        self.queue = self._build_queue(roots)

    def _build_queue(self, roots):
        """Create a depth first queue with order for multiple roots"""
        _queue = OrderedDict()
        def _add_to(mod, q):
            if (len(mod.resolvedRequiresDS) > 0):
                for m in mod.resolvedRequiresDS:
                    _add_to(m, q)
            q.update({mod: True})
        for m in roots:
            _add_to(m, _queue)

        return [m for m in _queue]

    def dfs_visit(self, action, state):
        """Depth first visit"""
        for m in self.queue:
            action(m, state)
    
    def bfs_visit(self, action, state):
        """Breadth first visit"""
        for m in reversed(self.queue):
            action(m, state)
