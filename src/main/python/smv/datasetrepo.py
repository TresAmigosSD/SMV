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
import itertools
import inspect
import pkgutil
import sys
import traceback

from smv.error import SmvRuntimeError
from smv.utils import smv_copy_array
from smv.py4j_interface import create_py4j_interface_method

"""Python implementations of IDataSetRepoPy4J and IDataSetRepoFactoryPy4J interfaces
"""

class DataSetRepoFactory(object):
    def __init__(self, smvApp):
        self.smvApp = smvApp

    def createRepo(self):
        return DataSetRepo(self.smvApp)

    getCreateRepo = create_py4j_interface_method("getCreateRepo", "createRepo")

    class Java:
        implements = ['org.tresamigos.smv.IDataSetRepoFactoryPy4J']


class DataSetRepo(object):
    def __init__(self, smvApp):
        self.smvApp = smvApp
        # Remove client modules from sys.modules to force reload of all client
        # code in the new transaction
        self._clear_sys_modules()

    def _clear_sys_modules(self):
        """
            Clear all client modules from sys.modules
            If modules have names like 'stage1.stage2.file.mod', then we have to clear all of
            set( 'stage1', 'stage1.stage2', 'stage1.stage2.file', 'stage1.stage2.file.mod' )
            from the sys.modules dictionary to avoid getting cached modules from python when
            we contruct a new DSR.
        """
        # { 'stage1' } from our example
        fqn_stubs_to_remove = { fqn.split('.')[0] for fqn in self.smvApp.stages() }

        for loaded_mod_fqn in list(sys.modules.keys()):
            for stubbed_fqn in fqn_stubs_to_remove:
                if loaded_mod_fqn == stubbed_fqn or loaded_mod_fqn.startswith(stubbed_fqn + '.'):
                    sys.modules.pop(loaded_mod_fqn)


    def _iter_submodules(self, stages):
        """Yield the names of all submodules of the packages corresponding to the given stages
        """
        file_iters_by_stage = (self._iter_submodules_in_stage(stage) for stage in stages)
        file_iter = itertools.chain(*file_iters_by_stage)
        return (name for (_, name, is_pkg) in file_iter if not is_pkg)

    def _iter_submodules_in_stage(self, stage):
        """Yield info on the submodules of the package corresponding with a given stage
        """
        try:
            stagemod = __import__(stage)
            for name in stage.split('.')[1:]:
                stagemod = getattr(stagemod, name)
        except:
            self.smvApp.log.warn("Package does not exist for stage: " + stage)
            return []
        # `walk_packages` can generate AttributeError if the system has
        # Gtk modules, which are not designed to use with reflection or
        # introspection. Best action to take in this situation is probably
        # to simply suppress the error.
        def onerror(name):
            self.smvApp.log.error("Skipping due to error during walk_packages: " + name)
        res = list(pkgutil.walk_packages(stagemod.__path__, stagemod.__name__ + '.'))
        return res

    def _for_name(self, name):
        """Dynamically load a module in a stage by its name.

            Similar to Java's Class.forName, but only looks in configured stages.
        """
        lastdot = name.rfind('.')
        file_name = name[ : lastdot]
        mod_name = name[lastdot+1 : ]

        mod = None

        # if file doesnt exist, module doesn't exist
        if file_name in self._iter_submodules(self.smvApp.stages()):
            # __import__ instantiates the module hierarchy but returns the root module
            f = __import__(file_name)
            # iterate to get the file that should contain the desired module
            for subname in file_name.split('.')[1:]:
                f = getattr(f, subname)
            # leave mod as None if the file exists but doesnt have an attribute with that name
            if hasattr(f, mod_name):
                mod = getattr(f, mod_name)

        return mod

    # Implementation of IDataSetRepoPy4J loadDataSet, which loads the dataset
    # from the most recent source. If the dataset does not exist, returns None.
    # However, if there is an error (such as a SyntaxError) which prevents the
    # user's file from being imported, the error will propagate back to the
    # DataSetRepoPython.
    def loadDataSet(self, fqn):
        ds = None
        ds_class = self._for_name(fqn)

        if ds_class is not None:
            ds = ds_class(self.smvApp)

            # Python issue https://bugs.python.org/issue1218234
            # need to invalidate inspect.linecache to make dataset hash work
            srcfile = inspect.getsourcefile(ds_class)
            if srcfile:
                inspect.linecache.checkcache(srcfile)

        return ds

    getLoadDataSet = create_py4j_interface_method("getLoadDataSet", "loadDataSet")


    def _dataSetsForStage(self, stageName):
        urns = []

        self.smvApp.log.debug("Searching for SmvDataSets in stage " + stageName)
        self.smvApp.log.debug("sys.path=" + repr(sys.path))

        for pymod_name in self._iter_submodules([stageName]):
            # The additional "." is necessary to prevent false positive, e.g. stage_2.M1 matches stage
            if pymod_name.startswith(stageName + "."):
                # __import__('a.b.c') returns the module a, just like import a.b.c
                pymod = __import__(pymod_name)
                # After import a.b.c we got a. Now we traverse from a to b to c
                for c in pymod_name.split('.')[1:]:
                    pymod = getattr(pymod, c)

                self.smvApp.log.debug("Searching for SmvDataSets in " + repr(pymod))

                # iterate over the attributes of the module, looking for SmvDataSets
                for obj_name in dir(pymod):
                    obj = getattr(pymod, obj_name)
                    self.smvApp.log.debug("Inspecting {} ({})".format(obj_name, type(obj)))
                    # We try to access the IsSmvDataSet attribute of the object.
                    # if it does not exist, we will catch the the AttributeError
                    # and skip the object, as it is not an SmvDataSet. We
                    # specifically check that IsSmvDataSet is identical to
                    # True, because some objects like Py4J's JavaObject override
                    # __getattr__ to **always** return something (so IsSmvDataSet
                    # maybe truthy even though the object is not an SmvDataSet).
                    try:
                        obj_is_smv_dataset = (obj.IsSmvDataSet is True)
                    except AttributeError:
                        obj_is_smv_dataset = False

                    if not obj_is_smv_dataset:
                        self.smvApp.log.debug("Ignoring {} because it is not an "
                                              "SmvDataSet".format(obj_name))
                        continue

                    # Class should have an fqn which begins with the stageName.
                    # Each package will contain all of the modules, classes, etc.
                    # that were imported into it, and we need to exclude these
                    # (so that we only count each module once)
                    obj_declared_in_stage = obj.fqn().startswith(pymod_name)

                    if not obj_declared_in_stage:
                        self.smvApp.log.debug("Ignoring {} because it was not "
                                              "declared in {}. (Note: it may "
                                              "be collected from another stage)"
                                              .format(obj_name, pymod_name))
                        continue

                    # Class should not be an ABC
                    obj_is_abstract = inspect.isabstract(obj)

                    if obj_is_abstract:
                        # abc labels methods as abstract via the attribute __isabstractmethod__
                        is_abstract_method = lambda attr: getattr(attr, "__isabstractmethod__", False)
                        abstract_methods = [name for name, _ in inspect.getmembers(obj, is_abstract_method)]
                        self.smvApp.log.debug("Ignoring {} because it is abstract ({} undefined)"
                                              .format(obj_name, ", ".join(abstract_methods)))

                        continue

                    self.smvApp.log.debug("Collecting " + obj_name)
                    urns.append(obj.urn())

        return urns

    def dataSetsForStage(self, stageName):
        urns = self._dataSetsForStage(stageName)
        return smv_copy_array(self.smvApp.sc, *urns)

    getDataSetsForStage = create_py4j_interface_method("getDataSetsForStage", "dataSetsForStage")

    def notFound(self, modUrn, msg):
        raise ValueError("dataset [{0}] is not found in {1}: {2}".format(modUrn, self.__class__.__name__, msg))

    class Java:
        implements = ['org.tresamigos.smv.IDataSetRepoPy4J']
