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
from smv.utils import lazy_property
from collections import OrderedDict
import sys

if sys.version_info >= (3, 0):
    import queue
else:
    import Queue as queue

class ModulesVisitor(object):
    """Provides way to do depth and breadth first visit to the sub-graph
        of modules given a set of roots
    """
    def __init__(self, roots):
        self.roots = roots
        self.queue = self._build_queue(roots)

    def _build_dict(self, roots, un_persisted_only):
        """Create a breadth first ordered dict for multiple roots,
            when un_persisted_only==False
            including all the modules up stream of the roots
            otherwise
            only include modules needed to calculate roots, in other
            words if a module is persisted already, its upper steam
            modules will be excluded
         """

        # to traversal the graph in bfs order
        _working_queue = queue.Queue()
        for m in roots:
            _working_queue.put(m)

        # keep a distinct sorted list of nodes with root always in front of leafs
        _sorted = OrderedDict()
        for m in roots:
            _sorted.update({m: True})

        while(not _working_queue.empty()):
            mod = _working_queue.get()
            if(not un_persisted_only or not mod._is_persisted()):
                for m in mod.resolvedRequiresDS:
                    # regardless whether seen before, add to queue, so not drop
                    # any dependency which may change the ordering of the result
                    _working_queue.put(m)

                    # if in the result list already, remove the old, add the new,
                    # to make sure leafs always later
                    if (m in _sorted):
                        _sorted.pop(m)
                    _sorted.update({m: True})

        return _sorted

    def _build_queue(self, roots):
        """Create a depth first queue for multiple roots"""
        _sorted = self._build_dict(roots, False)
        # reverse the result before output to make leafs first
        return [m for m in reversed(_sorted)]

    @lazy_property
    def modules_needed_for_run(self):
        """For each run, if a module is persisted, all its ancestors
            are not even needed to be visited. This method creates a
            sub-list for the queue which are needed for current run
        """
        _sorted = self._build_dict(self.roots, True)
        return [m for m in reversed(_sorted)]

    def dfs_visit(self, action, state, need_to_run_only=False):
        """Depth first visit"""
        if (need_to_run_only):
            l = self.modules_needed_for_run
        else:
            l = self.queue
        for m in l:
            action(m, state)

    def bfs_visit(self, action, state, need_to_run_only=False):
        """Breadth first visit"""
        if (need_to_run_only):
            l = self.modules_needed_for_run
        else:
            l = self.queue
        for m in reversed(l):
            action(m, state)
