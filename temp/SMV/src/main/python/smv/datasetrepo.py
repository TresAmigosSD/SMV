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

import smv
from smv.error import SmvRuntimeError
from smv.utils import smv_copy_array, lazy_property

"""Python implementations of IDataSetRepoPy4J and IDataSetRepoFactoryPy4J interfaces
"""

class DataSetRepoFactory(object):
    def __init__(self, smvApp):
        self.smvApp = smvApp

    def createRepo(self):
        return DataSetRepo(self.smvApp)

class DataSetRepo(object):
    def __init__(self, smvApp):
        self.smvApp = smvApp
        # When SmvApp init with py_module_hotload flag (by default),
        # remove client modules from sys.modules to force reload of all client
        # code in the new transaction
        if (smvApp.py_module_hotload):
            self._clear_sys_modules()

    def _clear_sys_modules(self):
        """
            Clear all client modules from sys.modules
            If modules have names like 'stage1.stage2.file.mod', then we have to clear all of
            set( 'stage1', 'stage1.stage2', 'stage1.stage2.file', 'stage1.stage2.file.mod' )
            from the sys.modules dictionary to avoid getting cached modules from python when
            we contruct a new DSR.
        """
        # The set of all user-defined code that needs to be decached
        # { 'stage1' } from our example
        user_code_fqns = set(self.smvApp.stages()).union(self.smvApp.userLibs())
        fqn_stubs_to_remove = {fqn.split('.')[0] for fqn in user_code_fqns}

        for loaded_mod_fqn in list(sys.modules.keys()):
            for stubbed_fqn in fqn_stubs_to_remove:
                if loaded_mod_fqn == stubbed_fqn or loaded_mod_fqn.startswith(stubbed_fqn + '.'):
                    sys.modules.pop(loaded_mod_fqn)

    def _for_name(self, name):
        """Dynamically load a module in a stage by its name.

            Similar to Java's Class.forName, but only looks in configured stages.
        """
        lastdot = name.rfind('.')
        file_name = name[ : lastdot]
        ds_name = name[lastdot+1 : ]

        ds = None

        # if file isn't discoverable, module doesn't exist
        if file_name in self.all_project_pymodules:
            pymod = self.all_project_pymodules[file_name]
            # leave ds as None if the file exists but doesnt have an attribute with that name
            if hasattr(pymod, ds_name):
                ds = getattr(pymod, ds_name)

        return ds

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

    def load_pymodule(self, fqn):
        mod = __import__(fqn)
        for subname in fqn.split('.')[1:]:
            mod = getattr(mod, subname)
        return mod

    @lazy_property
    def all_project_pymodules(self):
        """An index of discoverable Python modules by fqn

            A Python module is discoverable if it is importable and belongs to a stage. We cache this information
            because walk_packages is slow and walking packages repeatedly while loading many datasets explodes
            the running time of operations like getting the graph of a project.
        """
        def packages_in_stage(stage_name):
            stage_pymod = self.load_pymodule(stage_name)

            # where to recursively search for pymodules
            search_path = stage_pymod.__path__

            # Prefix of all pymodules and packages found in this dir. This is a little strange - suppose we are
            # searching in stage foo.bar which has the following structure:
            # |-foo
            #   |-bar
            #     |-buzz.py
            #     |-baz
            #       |-file.py
            # When walk_packages finds the directory `baz`, it won't know that the package's name is `foo.bar.baz` -
            # it's not aware that bar is contained within another package. Unless we provide a prefix, it will think
            # that `foo.bar.baz's` name is just `baz`.  This sort of makes sense, because if you added foo/bar to the
            # path then you could `import baz`. However, walk_packages will actually fail because it cannot
            # `import baz`, which it needs to do in order to get package details that inform the recursive search.
            # If there are no packages (only pymodules) in foo/bar, then `walk_packages` will succeed, but the output
            # names will be wrong (e.g. `buzz` instead of `foo.bar.buzz`).

            # `walk_packages` can generate AttributeError if the system has
            # Gtk modules, which are not designed to use with reflection or
            # introspection. Best action to take in this situation is probably
            # to simply suppress the error.
            def onerror(name):
                smv.logger.error("Skipping due to error during walk_packages: " + name)

            return pkgutil.walk_packages(
                path=search_path,
                prefix=stage_name + '.',
                onerror=onerror)

        stage_walker = itertools.chain(*(packages_in_stage(stage) for stage in self.smvApp.stages()))

        module_iter = (self.load_pymodule(name) for (_, name, is_pkg) in stage_walker if not is_pkg)

        return {pymod.__name__: pymod for pymod in module_iter}

    def _matchingClassesInPyModule(self, pymod, is_matching, skip_abs=True):
        """Finds all matching classes in a given python module.

           `is_matching` is called on each candidate object in the module.  Only non-abstract
           classes where `is_matching` returned true are returned.
        """
        matching_classes = []
        pymod_name = pymod.__name__

        # iterate over the attributes of the module, looking for SmvGenericModules
        for obj_name in dir(pymod):
            obj = getattr(pymod, obj_name)
            smv.logger.debug("Inspecting {} ({})".format(obj_name, type(obj)))

            # skip non-class objects
            if not inspect.isclass(obj):
                continue

            if not is_matching(obj):
                smv.logger.debug("Ignoring {} because it is not a match.".format(obj_name))
                continue

            obj_fqn = obj.__module__ + "." + obj.__name__
            # Class should have an fqn which begins with the module name.
            # Each package will contain all of the modules, classes, etc.
            # that were imported into it, and we need to exclude these
            # (so that we only count each module once)
            obj_declared_in_pymod = obj_fqn.startswith(pymod_name)

            if not obj_declared_in_pymod:
                smv.logger.debug("Ignoring {} because it was not "
                                        "declared in {}. (Note: it may "
                                        "be collected from another module)"
                                        .format(obj_name, pymod_name))
                continue

            # Class should not be an ABC
            obj_is_abstract = inspect.isabstract(obj)

            if skip_abs and obj_is_abstract:
                # abc labels methods as abstract via the attribute __isabstractmethod__
                is_abstract_method = lambda attr: getattr(attr, "__isabstractmethod__", False)
                abstract_methods = [name for name, _ in inspect.getmembers(obj, is_abstract_method)]
                smv.logger.debug("Ignoring {} because it is abstract ({} undefined)"
                                        .format(obj_name, ", ".join(abstract_methods)))

                continue

            smv.logger.debug("Collecting " + obj_name)
            matching_classes.append(obj)

        return matching_classes

    def dataSetsForStage(self, stageName):
        def is_generic_module(klass):
            # We try to access the IsSmvDataSet attribute of the object.
            # if it does not exist, we will catch the the AttributeError
            # and skip the object, as it is not an SmvGenericModules. We
            # specifically check that IsSmvDataSet is identical to
            # True, because some objects like Py4J's JavaObject override
            # __getattr__ to **always** return something (so IsSmvDataSet
            # maybe truthy even though the object is not an SmvGenericModules).
            try:
                klass_is_smv_dataset = (klass.IsSmvDataSet is True)
            except AttributeError:
                klass_is_smv_dataset = False
            return klass_is_smv_dataset

        fqns = []

        smv.logger.debug("Searching for SmvGenericModules in stage " + stageName)
        smv.logger.debug("sys.path=" + repr(sys.path))

        for pymod_name, pymod in self.all_project_pymodules.items():
            # The additional "." is necessary to prevent false positive, e.g. stage_2.M1 matches stage
            if pymod_name.startswith(stageName + "."):
                smv.logger.debug("Searching for SmvGenericModules in " + repr(pymod))

                gen_modules = self._matchingClassesInPyModule(pymod, is_generic_module)
                fqns.extend([obj.fqn() for obj in gen_modules])

        return fqns

    def _all_providers(self):
        """scans user libraries and smv libraries for "provider" classes.
            Returns list of discovered provider classes
        """
        def is_provider(klass):
            """A class is a provider if it has `IS_PROVIDER` and is not the base `SmvProvider`
               which returns empty string for provider type.
            """
            try:
                klass_is_provider = (klass.IS_PROVIDER is True) and (klass.provider_type())
            except AttributeError:
                klass_is_provider = False
            return klass_is_provider

        # providers can be in user libs dir or builtin smv
        prov_libs_names = self.smvApp.userLibs() + self.smvApp.semiLibs() + self.smvApp.smvLibs()
        prov_dict = {}

        for prov_lib_name in prov_libs_names:
            prov_lib = self.load_pymodule(prov_lib_name)
            providers = self._matchingClassesInPyModule(prov_lib, is_provider, skip_abs=False)
            for p in providers:
                p_fqn = p.provider_type_fqn()
                if p_fqn in prov_dict:
                    raise SmvRuntimeError("multiple providers with same fqn: " + p_fqn)
                prov_dict[p_fqn] = p

        return prov_dict
