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
import smv
from smv.modulesvisitor import ModulesVisitor
from smv.smviostrategy import SmvCsvPersistenceStrategy, SmvJsonOnHdfsPersistenceStrategy
from smv.smvmetadata import SmvMetaHistory
from smv.runinfo import SmvRunInfoCollector
from smv.utils import scala_seq_to_list, is_string
from smv.error import SmvRuntimeError, SmvMetadataValidationError

class SmvModuleRunner(object):
    """Represent the run-transaction. Provides the single entry point to run
        a group of modules
    """
    def __init__(self, modules, smvApp):
        self.roots = modules
        self.smvApp = smvApp
        self.log = smv.logger
        self.visitor = ModulesVisitor(modules)

    def run(self, forceRun=False):
        # a set of modules which need to run post_action, keep tracking
        # to make sure post_action run one and only one time for each TX
        # the set will be updated by _create_df, _create_meta and _force_post
        # and eventually be emptied out
        # See docs/dev/SmvGenericModule/SmvModuleRunner.md for details
        mods_to_run_post_action = set(self.visitor.modules_needed_for_run)

        # a map from fqn to already run DF, since the `run` interface of
        # SmvModule takes a map of class => df, the map here have to be
        # keyed by class method instead of `versioned_fqn`, which is only
        # in the resolved instance
        known = {}

        collector = SmvRunInfoCollector()

        # Do the real module calculation, when there are persistence, run
        # the post_actions and ancestor ephemeral modules post actions
        self._create_df(known, mods_to_run_post_action, collector, forceRun)

        # If there are ephemeral modules who has no persisting module
        # down stream, (must be part of roots), force an action and run
        # post actions
        self._force_post(mods_to_run_post_action, collector)

        dfs = [m.data for m in self.roots]
        return (dfs, collector)

    def quick_run(self, forceRun=False):
        known = {}
        collector = SmvRunInfoCollector()
        self._create_df(known, set(), forceRun, is_quick_run=True)
        dfs = [m.data for m in self.roots]
        return (dfs, collector)

    def get_runinfo(self):
        collector = SmvRunInfoCollector()
        def add_to_coll(m, _collector):
            hist = self.smvApp._read_meta_hist(m)
            _collector.add_runinfo(m.fqn(), m._get_metadata(), hist)
        self.visitor.dfs_visit(add_to_coll, collector, need_to_run_only=True)
        return collector

    # TODO: All the publish* methods below should be removed when move to generic output module
    def publish(self, publish_dir=None):
        # run before publish
        self.run()

        if (publish_dir is None):
            pubdir = self.smvApp.all_data_dirs().publishDir
            version = self.smvApp.all_data_dirs().publishVersion
            publish_dir = "{}/{}".format(pubdir, version)

        for m in self.roots:
            publish_base_path = "{}/{}".format(publish_dir, m.fqn())
            publish_csv_path = publish_base_path + ".csv"
            publish_meta_path = publish_base_path + ".meta"
            publish_hist_path = publish_base_path + ".hist"

            SmvCsvPersistenceStrategy(m.smvApp, m.fqn(), publish_csv_path).write(m.data)
            SmvJsonOnHdfsPersistenceStrategy(m.smvApp, publish_meta_path).write(m.module_meta.toJson())
            hist = self.smvApp._read_meta_hist(m)
            SmvJsonOnHdfsPersistenceStrategy(m.smvApp, publish_hist_path).write(hist.toJson())

    def publish_to_hive(self):
        # run before publish
        self.run()

        for m in self.roots:
            m.exportToHive()

    def publish_to_jdbc(self):
        self.run()

        for m in self.roots:
            m.publishThroughJDBC()

    def publish_local(self, local_dir):
        self.run()

        for m in self.roots:
            csv_path = "{}/{}".format(local_dir, m.versioned_fqn)
            m.data.smvExportCsv(csv_path)

    def purge_persisted(self):
        def cleaner(m, state):
            m.persistStrategy().remove()
            m.metaStrategy().remove()
        self.visitor.dfs_visit(cleaner, None)

    def _create_df(self, known, need_post, collector, forceRun=False, is_quick_run=False):
        # run module and create df. when persisting, post_action
        # will run on current module and all upstream modules
        def runner(m, state):
            (fqn2df, run_set, collector) = state
            m._do_it(fqn2df, run_set, collector, forceRun, is_quick_run)
        self.visitor.dfs_visit(runner, (known, need_post, collector), need_to_run_only=True)

    def _force_post(self, need_post, collector):
        # If there are still module left for post_action, force a run here
        # to run them and all left over on their upstream
        if (len(need_post) > 0):
            self.log.debug("leftover mods need to run post action: {}".format(
                [m.fqn()  for m in need_post]
            ))
            def force_run(mod, state):
                (run_set, coll) = state
                mod._force_post_action(run_set, coll)
            # Note: we used bfs_visit here run downstream first
            # In case of A<-B<-C all need to run, this way will only
            # need to force action on C, and A and B's post action can
            # also be calculated
            self.visitor.bfs_visit(force_run, (need_post, collector), need_to_run_only=True)
