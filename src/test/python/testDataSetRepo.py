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

    def test_repo_compiles_module_only_once(self):
        """DataSetRepo should not recompile module twice in a transaction

            Loading an SmvDataSet should only cause a recompile of its module
            if the module has not been imported previously in this transaction
        """
        dsr = self.build_new_repo()
        with self.ExtraPath("src/test/python/data_set_repo_1"):
            dsr.loadDataSet("stage.modules.CompileOnceA")
            # Get the first SmvDataSet class from its module
            dsA1 = getattr(sys.modules["stage.modules"], "CompileOnceA")
            # Load a different SmvDataSet from the same file
            dsr.loadDataSet("stage.modules.CompileOnceB")
            # Get the first SmvDataSet from the second SmvDataSet's module.
            # If the module wasn't recompiled then these should be equal.
            dsA2 = getattr(sys.modules["stage.modules"], "CompileOnceA")
            # Note that the module `sys.modules["stage.modules"]` won't change
            # identity (at least in Python 2.7), but its attributes will

        self.assertEqual(dsA1, dsA2)
