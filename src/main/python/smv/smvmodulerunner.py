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

from smv.modulesvisitor import ModulesVisitor
from smv.smviostrategy import SmvJsonOnHdfsIoStrategy

class SmvModuleRunner(object):
    """Represent the run-transaction. Provides the single entry point to run
        a group of modules
    """
    def __init__(self, modules, smvApp):
        self.roots = modules
        self.smvApp = smvApp
        self.log = smvApp.log
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

        self._create_df(known, mods_to_run_post_action)

        self._create_meta(mods_to_run_post_action)

        self._force_post(mods_to_run_post_action)

        self._persist_meta()

        return [known.get(m.urn()) for m in self.roots]

    def _create_df(self, known, need_post):
        def runner(m, state):
            (urn2df, run_set) = state
            m.rdd(urn2df, run_set)
        self.visitor.dfs_visit(runner, (known, need_post))

    def _create_meta(self, need_post):
        def run_meta(m, run_set):
            m.calculate_user_meta(run_set)
        self.visitor.dfs_visit(run_meta, need_post)

    def _force_post(self, need_post):
        if (len(need_post) > 0):
            self.log.debug("leftover mods need to run post action: {}".format(
                [m.fqn()  for m in need_post]
            ))
            def force_run(mod, run_set):
                mod.force_post_action(run_set)
            self.visitor.bfs_visit(force_run, need_post)

    def _persist_meta(self):
        def write_meta(m, stage):
            meta_json = m.module_meta.toJson()
            SmvJsonOnHdfsIoStrategy(
                self.smvApp, m.meta_path()
            ).write(meta_json)
        self.visitor.dfs_visit(write_meta, None)