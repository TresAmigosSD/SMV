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
from smv.smviostrategy import SmvCsvOnHdfsIoStrategy, SmvJsonOnHdfsIoStrategy
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
        self.log = smvApp.log
        self.visitor = ModulesVisitor(modules)

    def run(self, forceRun=False):
        # a set of modules which need to run post_action, keep tracking 
        # to make sure post_action run one and only one time for each TX
        # the set will be updated by _create_df, _create_meta and _force_post
        # and eventually be emptied out
        # See docs/dev/SmvGenericModule/SmvModuleRunner.md for details
        mods_to_run_post_action = set(self.visitor.queue)

        # a map from urn to already run DF, since the `run` interface of 
        # SmvModule takes a map of class => df, the map here have to be 
        # keyed by class method instead of `versioned_fqn`, which is only 
        # in the resolved instance
        known = {}

        collector = SmvRunInfoCollector()

        self._create_df(known, mods_to_run_post_action, forceRun)

        self._create_meta(mods_to_run_post_action)

        if (self.smvApp.py_smvconf.force_edd()):
            self._create_edd(mods_to_run_post_action)

        self._force_post(mods_to_run_post_action)

        self._validate_and_persist_meta(collector)

        dfs = [m.data for m in self.roots]
        return (dfs, collector)

    def quick_run(self, forceRun=False):
        known = {}
        self._create_df(known, set(), forceRun, is_quick_run=True)
        return [m.data for m in self.roots]

    def get_runinfo(self):
        collector = SmvRunInfoCollector()
        def add_to_coll(m, _collector):
            hist = self.smvApp._read_meta_hist(m)
            _collector.add_runinfo(m.fqn(), m.get_metadata(), hist)
        self.visitor.dfs_visit(add_to_coll, collector)
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

            SmvCsvOnHdfsIoStrategy(m.smvApp, m.fqn(), None, publish_csv_path).write(m.data)
            SmvJsonOnHdfsIoStrategy(m.smvApp, publish_meta_path).write(m.module_meta.toJson())
            hist = self.smvApp._read_meta_hist(m)
            SmvJsonOnHdfsIoStrategy(m.smvApp, publish_hist_path).write(hist.toJson())

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

    def _create_df(self, known, need_post, forceRun=False, is_quick_run=False):
        # run module and create df. when persisting, post_action 
        # will run on current module and all upstream modules
        def runner(m, state):
            (urn2df, run_set) = state
            m.rdd(urn2df, run_set, forceRun, is_quick_run)
        self.visitor.dfs_visit(runner, (known, need_post))

    def _create_meta(self, need_post):
        # run user meta. when there is actions in user meta creation,
        # post_action will run on current module and all upstream modules
        def run_meta(m, run_set):
            m.calculate_user_meta(run_set)
        self.visitor.dfs_visit(run_meta, need_post)

    def _create_edd(self, need_post):
        def run_edd(m, run_set):
            m.calculate_edd(run_set)
        self.visitor.dfs_visit(run_edd, need_post)

    def _force_post(self, need_post):
        # If there are still module left for post_action, force a run here
        # to run them and all left over on their upstream
        if (len(need_post) > 0):
            self.log.debug("leftover mods need to run post action: {}".format(
                [m.fqn()  for m in need_post]
            ))
            def force_run(mod, run_set):
                mod.force_post_action(run_set)
            # Note: we used bfs_visit here run downstream first
            # In case of A<-B<-C all need to run, this way will only
            # need to force action on C, and A and B's post action can 
            # also be calculated
            self.visitor.bfs_visit(force_run, need_post)

    def _validate_and_persist_meta(self, collector):
        def do_validation(m, _collector):
            io_strategy = m.metaStrategy()
            persisted = io_strategy.isPersisted()
            # If persisted already, skip: validation, update hist, populate runinfo_collector
            if (not persisted):
                # Validate
                m.finalize_meta()
                hist = self.smvApp._read_meta_hist(m)
                res = m.validateMetadata(m.module_meta, hist)
                if res is not None and not is_string(res):
                    raise SmvRuntimeError("Validation failure message {} is not a string".format(repr(res)))
                if (is_string(res) and len(res) > 0):
                    raise SmvMetadataValidationError(res)
                # Persist Meta
                m.persist_meta()
                # Populate collector
                _collector.add_runinfo(m.fqn(), m.module_meta, hist)
                # Update meta history and persist
                hist.update(m.module_meta, m.metadataHistorySize())
                io_strategy = self.smvApp._hist_io_strategy(m)
                io_strategy.write(hist.toJson())
        self.visitor.dfs_visit(do_validation, collector)