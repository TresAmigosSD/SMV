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

"""SmvPy entry class and ``singleton``smvApp
This module provides the main SMV Python entry point ``SmvPy`` class and a singleton `smvApp`.
It is equivalent to ``SmvApp`` on Scala side
"""

from py4j.java_gateway import java_import, JavaObject

from pyspark import SparkContext
from pyspark.sql import SparkSession, HiveContext, DataFrame
from utils import for_name, smv_copy_array, check_socket
from error import SmvRuntimeError

import inspect
import pkgutil
import os
import re
import sys
import traceback

if sys.version >= '3':
    basestring = unicode = str
    long = int
    from io import StringIO
    from importlib import reload
else:
    from cStringIO import StringIO

class SmvApp(object):
    """The Python representation of SMV.

    Its singleton instance is created later in the containing module
    and is named smvApp

    Adds `java_imports` to the namespace in the JVM gateway in
    SparkContext (in pyspark).  It also creates an instance of
    SmvPyClient.

    """

    # Singleton instance of SmvApp
    _instance = None

    @classmethod
    def getInstance(cls):
        if cls._instance is None:
            raise SmvRuntimeError("An instance of SmvApp has not been created")
        else:
            return cls._instance

    @classmethod
    def createInstance(cls, arglist, _sparkSession = None):
        """Create singleton instance. Also returns the instance.
        """
        cls._instance = cls(arglist, _sparkSession)
        return cls._instance

    def __init__(self, arglist, _sparkSession = None):
        self.sparkSession = SparkSession.builder. \
                                enableHiveSupport(). \
                                getOrCreate() if _sparkSession is None else _sparkSession
        sc = self.sparkSession.sparkContext
        sc.setLogLevel("ERROR")

        self.sc = sc

        self.sqlContext = self.sparkSession._wrapped

        self.prepend_source("src/main/python")
        self._jvm = self.sc._jvm

        from py4j.java_gateway import java_import
        java_import(self._jvm, "org.tresamigos.smv.ColumnHelper")
        java_import(self._jvm, "org.tresamigos.smv.SmvDFHelper")
        java_import(self._jvm, "org.tresamigos.smv.dqm.*")
        java_import(self._jvm, "org.tresamigos.smv.python.SmvPythonHelper")

        self.j_smvPyClient = self.create_smv_pyclient(arglist)

        # shortcut is meant for internal use only
        self.j_smvApp = self.j_smvPyClient.j_smvApp()

        # issue #429 set application name from smv config
        sc._conf.setAppName(self.appName())

        # user may choose a port for the callback server
        gw = sc._gateway
        cbsp = self.j_smvPyClient.callbackServerPort()
        cbs_port = cbsp.get() if cbsp.isDefined() else gw._python_proxy_port

        # check wither the port is in-use or not. Try 10 times, if all fail, error out
        check_counter = 0
        while(not check_socket(cbs_port) and check_counter < 10):
            cbs_port += 1
            check_counter += 1

        if (not check_socket(cbs_port)):
            raise SmvRuntimeError("Start Python callback server failed. Port {0}-{1} are all in use".format(cbs_port - check_counter, cbs_port))

        if "_callback_server" not in gw.__dict__ or gw._callback_server is None:
            print("Starting Py4j callback server on port {0}".format(cbs_port))
            gw.callback_server_parameters.eager_load = True
            gw.callback_server_parameters.daemonize = True
            gw.callback_server_parameters.daemonize_connections = True
            gw.callback_server_parameters.port = cbs_port
            gw.start_callback_server(gw.callback_server_parameters)
            gw._callback_server.port = cbs_port
            gw._python_proxy_port = gw._callback_server.port
            # get the GatewayServer object in JVM by ID
            jgws = JavaObject("GATEWAY_SERVER", gw._gateway_client)
            # update the port of CallbackClient with real port
            gw.jvm.SmvPythonHelper.updatePythonGatewayPort(jgws, gw._python_proxy_port)

        self.j_smvPyClient.registerRepoFactory('Python', DataSetRepoFactory(self))

    def appName(self):
        return self.j_smvApp.smvConfig().appName()

    def create_smv_pyclient(self, arglist):
        '''
        return a smvPyClient instance
        '''
        # convert python arglist to java String array
        java_args =  smv_copy_array(self.sc, *arglist)
        return self._jvm.org.tresamigos.smv.python.SmvPyClientFactory.init(java_args, self.sparkSession._jsparkSession)

    def get_graph_json(self):
        """Generate a json string representing the dependency graph.
           TODO: need to add a stageName parameter to limit it to a single stage.
        """
        return self.j_smvApp.generateAllGraphJSON()

    def runModule(self, urn):
        """Runs either a Scala or a Python SmvModule by its Fully Qualified Name(fqn)
        """
        jdf = self.j_smvPyClient.runModule(urn)
        return DataFrame(jdf, self.sqlContext)

    def runModuleByName(self, name):
        jdf = self.j_smvApp.runModuleByName(name)
        return DataFrame(jdf, self.sqlContext)

    def urn2fqn(self, urnOrFqn):
        """Extracts the SMV module FQN portion from its URN; if it's already an FQN return it unchanged"""
        return self.j_smvPyClient.urn2fqn(urnOrFqn)

    def outputDir(self):
        return self.j_smvPyClient.outputDir()

    def scalaOption(self, val):
        """Returns a Scala Option containing the value"""
        return self._jvm.scala.Option.apply(val)

    def createDF(self, schema, data = None):
        return DataFrame(self.j_smvPyClient.dfFrom(schema, data), self.sqlContext)

    def _mkCsvAttr(self, delimiter=',', quotechar='"', hasHeader=False):
        """Factory method for creating instances of Scala case class CsvAttributes"""
        return self._jvm.org.tresamigos.smv.CsvAttributes(delimiter, quotechar, hasHeader)

    def defaultCsvWithHeader(self):
        return self._mkCsvAttr(hasHeader=True)

    def defaultTsv(self):
        return self._mkCsvAttr(delimiter='\t')

    def defaultTsvWithHeader(self):
        return self._mkCsvAttr(delimier='\t', hasHeader=True)

    def prepend_source(self,source_dir):
        # Source must be added to front of path to make sure it is found first
        codePath = os.path.abspath(source_dir)
        sys.path.insert(1, codePath)

    def run(self):
        self.j_smvApp.run()

class DataSetRepoFactory(object):
    def __init__(self, smvApp):
        self.smvApp = smvApp
    def createRepo(self):
        try:
            return DataSetRepo(self.smvApp)
        except BaseException as e:
            traceback.print_exc()
            raise e

    class Java:
        implements = ['org.tresamigos.smv.IDataSetRepoFactoryPy4J']


class DataSetRepo(object):
    def __init__(self, smvApp):
        self.smvApp = smvApp
    # Implementation of IDataSetRepoPy4J loadDataSet, which loads the dataset
    # from the most recent source
    def loadDataSet(self, fqn):
        lastdot = fqn.rfind('.')
        try:
            if sys.modules.has_key(fqn[:lastdot]):
                # reload the module if it has already been imported
                return self._reload(fqn)
            else:
                # otherwise import the module
                return self._load(fqn)
        except BaseException as e:
            traceback.print_exc()
            raise e



    # Import the module (Python module, not SMV module) containing the dataset
    # and return the dataset
    def _load(self, fqn):
        return for_name(fqn)(self.smvApp)

    # Reload the module containing the dataset from the most recent source
    # and invalidate the linecache
    def _reload(self, fqn):
        lastdot = fqn.rfind('.')
        pmod = reload(sys.modules[fqn[:lastdot]])
        klass = getattr(pmod, fqn[lastdot+1:])
        ds = klass(self.smvApp)
        # Python issue https://bugs.python.org/issue1218234
        # need to invalidate inspect.linecache to make dataset hash work
        srcfile = inspect.getsourcefile(ds.__class__)
        if srcfile:
            inspect.linecache.checkcache(srcfile)
        return ds

    def dataSetsForStage(self, stageName):
        try:
            return self._moduleUrnsForStage(stageName, lambda obj: obj.IsSmvPyDataSet)
        except BaseException as e:
            traceback.print_exc()
            raise e

    def outputModsForStage(self, stageName):
        return self.moduleUrnsForStage(stageName, lambda obj: obj.IsSmvPyModule and obj.IsSmvPyOutput)

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
        try:
            stagemod = __import__(stageName)
        except:
            # may be a scala-only stage
            pass
        else:
            for loader, name, is_pkg in pkgutil.walk_packages(stagemod.__path__, stagemod.__name__ + '.' , onerror=err):
                # The additional "." is necessary to prevent false positive, e.g. stage_2.M1 matches stage
                if name.startswith(stageName + ".") and not is_pkg:
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
                            if fn(obj) and obj.fqn().startswith(name):
                                buf.append(obj.urn())
                        except AttributeError:
                            continue

        return smv_copy_array(self.smvApp.sc, *buf)

    def notFound(self, modUrn, msg):
        raise ValueError("dataset [{0}] is not found in {1}: {2}".format(modUrn, self.__class__.__name__, msg))

    class Java:
        implements = ['org.tresamigos.smv.IDataSetRepoPy4J']
