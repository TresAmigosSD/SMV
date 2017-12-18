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
import traceback
import pkgutil
import inspect

from smv.error import SmvRuntimeError
from smv.utils import for_name, smv_copy_array, iter_submodules
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
        """Clear all client modules from sys.modules
        """
        for fqn in list(sys.modules.keys()):
            for stage_name in self.smvApp.stages:
                if fqn == stage_name or fqn.startswith(stage_name + "."):
                    sys.modules.pop(fqn)
                    break

    # Implementation of IDataSetRepoPy4J loadDataSet, which loads the dataset
    # from the most recent source. If the dataset does not exist, returns None.
    # However, if there is an error (such as a SyntaxError) which prevents the
    # user's file from being imported, the error will propagate back to the
    # DataSetRepoPython.
    def loadDataSet(self, fqn):
        ds = None
        ds_class = for_name(fqn, self.smvApp.stages)

        if ds_class is not None:
            ds = ds_class(self.smvApp)

            # Python issue https://bugs.python.org/issue1218234
            # need to invalidate inspect.linecache to make dataset hash work
            srcfile = inspect.getsourcefile(ds_class)
            if srcfile:
                inspect.linecache.checkcache(srcfile)

        return ds

    getLoadDataSet = create_py4j_interface_method("getLoadDataSet", "loadDataSet")


    def dataSetsForStage(self, stageName):
        return self._moduleUrnsForStage(stageName, lambda obj: obj.IsSmvDataSet)

    getDataSetsForStage = create_py4j_interface_method("getDataSetsForStage", "dataSetsForStage")

    def _moduleUrnsForStage(self, stageName, fn):
        # `walk_packages` can generate AttributeError if the system has
        # Gtk modules, which are not designed to use with reflection or
        # introspection. Best action to take in this situation is probably
        # to simply suppress the error.
        def err(name): pass
        # print("Error importing module %s" % name)
        # t, v, tb = sys.exc_info()
        # print("type is {0}, value is {1}".format(t, v))
        buf = []
        # import the stage and only walk the packages in the path of that stage, recursively
        for name in iter_submodules([stageName]):
                # The additional "." is necessary to prevent false positive, e.g. stage_2.M1 matches stage
                if name.startswith(stageName + "."):
                    pymod = __import__(name)
                    for c in name.split('.')[1:]:
                        pymod = getattr(pymod, c)

                    for n in dir(pymod):
                        obj = getattr(pymod, n)
                        try:
                            # Class should have an fqn which begins with the stageName.
                            # Each package will contain among other things all of
                            # the modules that were imported into it, and we need
                            # to exclude these (so that we only count each module once)
                            # Also we need to exclude ABCs
                            if fn(obj) and obj.fqn().startswith(name) and not inspect.isabstract(obj):
                                buf.append(obj.urn())
                        except AttributeError:
                            continue

        return smv_copy_array(self.smvApp.sc, *buf)

    def notFound(self, modUrn, msg):
        raise ValueError("dataset [{0}] is not found in {1}: {2}".format(modUrn, self.__class__.__name__, msg))

    class Java:
        implements = ['org.tresamigos.smv.IDataSetRepoPy4J']
