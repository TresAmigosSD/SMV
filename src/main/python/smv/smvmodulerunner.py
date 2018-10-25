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

        self._create_df(known, mods_to_run_post_action, forceRun)

        self._create_meta(mods_to_run_post_action)

        self._force_post(mods_to_run_post_action)

        self._validate_meta()
        self._persist_meta()

        return [self._get_df_and_run_info(m) for m in self.roots]

    def quick_run(self):
        known = {}
        self._create_df(known, set(), is_quick_run=True)
        return [m.df for m in self.roots]

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

            SmvCsvOnHdfsIoStrategy(m.smvApp, m.fqn(), None, publish_csv_path).write(m.df)
            SmvJsonOnHdfsIoStrategy(m.smvApp, publish_meta_path).write(m.module_meta.toJson())
            hist = self._read_meta_hist(m)
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
            m.df.smvExportCsv(csv_path)

    def purge_persisted(self):
        def cleaner(m, state):
            m.persistStrategy().remove()
            m.metaStrategy().remove()
        self.visitor.dfs_visit(cleaner, None)

    def purge_old_but_keep_new_persisted(self):
        keep = []
        def get_to_keep(m, k):
            k.extend(m.persistStrategy().allOutput())
            k.extend(m.metaStrategy().allOutput())

        self.visitor.dfs_visit(get_to_keep, keep)
        outdir = self.smvApp.all_data_dirs().outputDir

        # since all file paths in keep are prefixed by outdir, remove it
        relative_paths = [i[len(outdir)+1:] for i in keep]

        res = self.smvApp._jvm.SmvPythonHelper.purgeDirectory(
            outdir,
            relative_paths
        )
        
        for r in scala_seq_to_list(self.smvApp._jvm, res):
            if (r.success()):
                self.log.info("... Deleted {}".format(r.fn()))
            else:
                self.log.info("... Unable to delete {}".format(r.fn()))

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

    def _validate_meta(self):
        def do_validation(m, state):
            m.finalize_meta()
            hist = self._read_meta_hist(m)
            res = m.validateMetadata(m.module_meta, hist)
            if res is not None and not is_string(res):
                raise SmvRuntimeError("Validation failure message {} is not a string".format(repr(res)))
            if (is_string(res) and len(res) > 0):
                raise SmvMetadataValidationError(res)
        self.visitor.dfs_visit(do_validation, None)

    def _hist_io_strategy(self, m):
        hist_dir = self.smvApp.all_data_dirs().historyDir
        hist_path = "{}/{}.hist".format(hist_dir, m.fqn())
        return SmvJsonOnHdfsIoStrategy(self.smvApp, hist_path)

    def _read_meta_hist(self, m):
        io_strategy = self._hist_io_strategy(m)
        try:
            hist_json = io_strategy.read()
            hist = SmvMetaHistory().fromJson(hist_json)
        except:
            hist = SmvMetaHistory()
        return hist

    def _persist_meta(self):
        def write_meta(m, state):
            persisted = m.persist_meta()
            if (not persisted):
                hist = self._read_meta_hist(m)
                hist.update(m.module_meta, m.metadataHistorySize())
                io_strategy = self._hist_io_strategy(m)
                io_strategy.write(hist.toJson())

        self.visitor.dfs_visit(write_meta, None)

    def _get_df_and_run_info(self, m):
        df = m.df
        meta = m.module_meta
        meta_hist = self._read_meta_hist(m)
        return (df, (meta, meta_hist))
