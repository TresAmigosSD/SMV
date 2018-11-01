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
from test_support.extrapath import ExtraPath

from smv.datasetrepo import DataSetRepo


class DataSetRepoTest(SmvBaseTest):
    @classmethod
    def smvAppInitArgs(cls):
        return ["--smv-props", "smv.stages=stage"]

    @classmethod
    def before_dir(cls):
        return cls.resourceTestDir() + "/before"

    @classmethod
    def after_dir(cls):
        return cls.resourceTestDir() + "/after"

    def build_new_repo(self):
        return DataSetRepo(self.smvApp)

    def test_discover_new_module_in_file(self):
        """DataSetRepo should discover SmvModules added to an existing file
        """
        with ExtraPath(self.before_dir()):
            # File should already have been imported before module is added
            self.build_new_repo().dataSetsForStage("stage")

        with ExtraPath(self.after_dir()):
            modules = list( self.build_new_repo().dataSetsForStage("stage") )

        self.assertTrue( "mod:stage.modules.NewModule" in modules, "mod:stage.modules.NewModule not in " + str(modules) )

    def test_repo_dslist_filter_abc(self):
        """DataSetRepo's dataSetsForStage method should not return ABC Classes
        """
        with ExtraPath(self.before_dir()):
            modules = self.build_new_repo().dataSetsForStage("stage")

        self.assertNotIn("mod:stage.abcmod.ABCMod", modules, "mod:stage.abcmod.ABCMod is in " + str(modules) )

    def test_repo_compiles_module_only_once(self):
        """DataSetRepo should not recompile module twice in a transaction

            Loading an SmvGenericModule should only cause a recompile of its module
            if the module has not been imported previously in this transaction.
            This applies even when loading different SmvGenericModules from the same file.
        """
        dsr = self.build_new_repo()
        with ExtraPath(self.before_dir()):
            dsA1 = dsr.loadDataSet("stage.modules.CompileOnceA").__class__
            # load a different SmvGenericModule from the same file
            dsr.loadDataSet("stage.modules.CompileOnceB")
            # get the first SmvGenericModule from the second SmvGenericModule's module
            # if the module wasn't recompiled these should be equal
            dsA2 = getattr(sys.modules["stage.modules"], "CompileOnceA")
            # note that the module `sys.modules["stage.modules"]` won't change
            # identity (at least in Python 2.7), but its attributes will

        self.assertEqual(dsA1, dsA2)

    def test_new_repo_reloads_base_class(self):
        """When DataSetRepo reloads an SmvGenericModule it should reload its client ABC (if any)

            Users may create ABCs (which may or may not actually use the abc module)
            for SmvModules. When an implementation SmvModule is imported for the
            first time in a transaction it should be recompiled, and it should also
            trigger the reload of the ABC even if the ABC lives in another file.
        """
        with ExtraPath(self.before_dir()):
            abcmod1 = self.build_new_repo().loadDataSet("stage.modules.ImplMod").__class__

        with ExtraPath(self.before_dir()):
            abcmod2 = self.build_new_repo().loadDataSet("stage.modules.ImplMod").__class__

        self.assertNotEqual(abcmod1, abcmod2)

    def test_ignore_JavaObjects(self):
        """When a file in a stage contains a Py4J JavaObject, DataSetRepo should ignore it

            Users may utilize Py4J to interact with Java code, and in doing so
            may create JavaObjects at the top level of a file. DataSetRepo should
            not interpret these JavaObjects as SmvDataSets, despite the fact that
            they have a truthy (not None) IsSmvDataSet attribute.

            Note: the reason why JavaObjects have an IsSmvDataSet attribute is
            that they override __getattr__ to **always** return something. The
            same thing is true of PySpark Columns and classes from other Python
            libraries.
        """
        # dir containing stage with module containing JavaObject
        java_obj_dir = self.resourceTestDir() + "/java_obj"
        with ExtraPath(java_obj_dir):
            mods_in_dir = self.build_new_repo()._dataSetsForStage("stage")

        self.assertEqual(mods_in_dir, ["mod:stage.modules.WhateverModule"])
