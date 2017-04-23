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
import sys

from test_support.smvbasetest import SmvBaseTest

from smv.datasetrepo import DataSetRepo

class DataSetRepoTest(SmvBaseTest):
    @classmethod
    def smvAppInitArgs(cls):
        return ["--smv-props", "smv.stages=stage"]

    class ExtraPath(object):
        def __init__(self, extra_path):
            self.extra_path = extra_path

        def __enter__(self):
            sys.path.insert(1, self.extra_path)

        def __exit__(self, type, value, traceback):
            sys.path.remove(self.extra_path)

    def build_new_repo(self): return DataSetRepo(self.smvApp)

    def test_discover_new_module_in_file(self):
        """DataSetRepo should discover SmvModules added to an existing file
        """
        with self.ExtraPath("src/test/python/data_set_repo_1"):
            # File should already have been imported before module is added
            self.build_new_repo().dataSetsForStage("stage")

        with self.ExtraPath("src/test/python/data_set_repo_2"):
            modules = list( self.build_new_repo().dataSetsForStage("stage") )

        self.assertTrue( "mod:stage.modules.NewModule" in modules, "mod:stage.modules.NewModule not in " + str(modules) )
