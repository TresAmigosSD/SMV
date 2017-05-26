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

"""SmvApp entry class
This module provides the main SMV Python entry point ``SmvPy`` class and a singleton `smvApp`.
It is equivalent to ``SmvApp`` on Scala side
"""
import os
import sys
import traceback

from py4j.java_gateway import java_import, JavaObject, CallbackServerParameters

from pyspark import SparkContext
from pyspark.sql import HiveContext, DataFrame
from utils import smv_copy_array, check_socket
from error import SmvRuntimeError

from datasetrepo import DataSetRepoFactory

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
    def createInstance(cls, arglist, _sc = None, _sqlContext = None):
        """Create singleton instance. Also returns the instance.
        """
        cls._instance = cls(arglist, _sc, _sqlContext)
        return cls._instance

    def __init__(self, arglist, _sc = None, _sqlContext = None):
        sc = SparkContext() if _sc is None else _sc
        sqlContext = HiveContext(sc) if _sqlContext is None else _sqlContext

        self.prepend_source("src/main/python")

        sc.setLogLevel("ERROR")

        self.sqlContext = sqlContext
        self.sc = sc
        self._jvm = sc._jvm

        from py4j.java_gateway import java_import
        java_import(self._jvm, "org.tresamigos.smv.ColumnHelper")
        java_import(self._jvm, "org.tresamigos.smv.SmvDFHelper")
        java_import(self._jvm, "org.tresamigos.smv.dqm.*")
        java_import(self._jvm, "org.tresamigos.smv.panel.*")
        java_import(self._jvm, "org.tresamigos.smv.python.SmvPythonHelper")

        self.j_smvPyClient = self.create_smv_pyclient(arglist)

        # shortcut is meant for internal use only
        self.j_smvApp = self.j_smvPyClient.j_smvApp()

        self.stages = self.j_smvPyClient.stages()

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
            gw.shutdown_callback_server() # in case another has already started
            gw.start_callback_server(CallbackServerParameters(
                port=cbs_port,
                daemonize=True,
                daemonize_connections=True
            ))
            gw._python_proxy_port = gw._callback_server.port
            # get the GatewayServer object in JVM by ID
            jgws = JavaObject("GATEWAY_SERVER", gw._gateway_client)
            # update the port of CallbackClient with real port
            gw.jvm.SmvPythonHelper.updatePythonGatewayPort(jgws, gw._python_proxy_port)

        self.j_smvPyClient.registerRepoFactory('Python', DataSetRepoFactory(self))

        # Suppress creation of .pyc files. These cause complications with
        # reloading code and have led to discovering deleted modules (#612)
        sys.dont_write_bytecode = True

    def appName(self):
        return self.j_smvApp.smvConfig().appName()

    def create_smv_pyclient(self, arglist):
        '''
        return a smvPyClient instance
        '''
        # convert python arglist to java String array
        java_args =  smv_copy_array(self.sc, *arglist)
        return self._jvm.org.tresamigos.smv.python.SmvPyClientFactory.init(java_args, self.sqlContext._ssql_ctx)

    def get_graph_json(self):
        """Generate a json string representing the dependency graph.
           TODO: need to add a stageName parameter to limit it to a single stage.
        """
        return self.j_smvApp.generateAllGraphJSON()

    def runModule(self, urn, forceRun = False, version = None):
        """Runs either a Scala or a Python SmvModule by its Fully Qualified Name(fqn)
        """
        jdf = self.j_smvPyClient.runModule(urn, forceRun, self.scalaOption(version))
        return DataFrame(jdf, self.sqlContext)

    def runModuleByName(self, name, forceRun = False, version = None):
        jdf = self.j_smvApp.runModuleByName(name, forceRun, self.scalaOption(version))
        return DataFrame(jdf, self.sqlContext)

    def urn2fqn(self, urnOrFqn):
        """Extracts the SMV module FQN portion from its URN; if it's already an FQN return it unchanged"""
        return self.j_smvPyClient.urn2fqn(urnOrFqn)

    def getStageFromModuleFqn(self, fqn):
        """Returns the stage name for a given fqn"""
        return self._jvm.org.tresamigos.smv.ModURN(fqn).getStage().get()

    def outputDir(self):
        return self.j_smvPyClient.outputDir()

    def scalaOption(self, val):
        """Returns a Scala Option containing the value"""
        return self._jvm.scala.Option.apply(val)

    def scalaNone(self):
        """Returns a Scala None value"""
        return self.scalaOption(None)

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
