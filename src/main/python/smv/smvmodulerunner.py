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
from smv.smvdataset import ModulesVisitor

class SmvModuleRunner(object):
    """Represent the run-transaction. Provides the single entry point to run
        a group of modules
    """
    def __init__(self, modules, log):
        self.roots = modules
        self.log = log
        self.visitor = ModulesVisitor(modules)

    def run(self):
        # a set of modules which need to run post_action, keep tracking 
        # to make sure post_action run one and only one time for each TX
        mods_to_run_post_action = set(self.visitor.queue)

        # a map from urn to already run DF, since the `run` interface of 
        # SmvModule takes a map of class => df, the map here have to be 
        # keyed by class method instead of `versioned_fqn`, which is only 
        # in the resolved instance
        known = {}

        def runner(m, (urn2df, run_set)):
            m.rdd(urn2df, run_set)
        self.visitor.dfs_visit(runner, (known, mods_to_run_post_action))


        if (len(mods_to_run_post_action) > 0):
            self.log.debug("leftover mods need to run post action: {}".format(
                [m.fqn()  for m in mods_to_run_post_action]
            ))
            def force_run(mod, run_set):
                mod.force_post_action(run_set)
            self.visitor.bfs_visit(force_run, mods_to_run_post_action)

        return [known.get(m.urn()) for m in self.roots]

