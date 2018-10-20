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
"""SMV User Run Configuration Parameters

This module defined the SmvRunConfig class which can be mixed-in into an
SmvModule to get user configuration parameters at run-time.
"""

from smv.smvdataset import ModulesVisitor

class SmvModuleRunner(object):
    def __init__(self, modules):
        self.roots = modules
        self.visitor = ModulesVisitor(modules)

    def run(self):
        known = {}
        def runner(m, urn2df):
            m.rdd(urn2df)
        self.visitor.dfs_visit(runner, known)
        return [known.get(m.urn()) for m in self.roots]

