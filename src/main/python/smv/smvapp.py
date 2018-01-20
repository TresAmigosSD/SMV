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

from py4j.java_gateway import java_import, JavaObject
from pyspark import SparkContext
from pyspark.sql import HiveContext, DataFrame


from smv.datasetrepo import DataSetRepoFactory
from smv.utils import smv_copy_array, check_socket
from smv.error import SmvRuntimeError
import smv.helpers
from smv.utils import FileObjInputStream
from smv.runinfo import SmvRunInfoCollector

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

    @classmethod
    def setInstance(cls, app):
        """Set the singleton instance.
        """
        cls._instance = app

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
        java_import(self._jvm, "org.tresamigos.smv.SmvRunInfoCollector")

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

        # this was a workaround for py4j 0.8.2.1, shipped with spark
        # 1.5.x, to prevent the callback server from hanging the
        # python, and hence the java, process
        from pyspark.streaming.context import _daemonize_callback_server
        _daemonize_callback_server()

        if "_callback_server" not in gw.__dict__ or gw._callback_server is None:
            print("SMV starting Py4j callback server on port {0}".format(cbs_port))
            gw._shutdown_callback_server() # in case another has already started
            gw._start_callback_server(cbs_port)
            gw._python_proxy_port = gw._callback_server.port
            # get the GatewayServer object in JVM by ID
            jgws = JavaObject("GATEWAY_SERVER", gw._gateway_client)
            # update the port of CallbackClient with real port
            gw.jvm.SmvPythonHelper.updatePythonGatewayPort(jgws, gw._python_proxy_port)

        self.repoFactory = DataSetRepoFactory(self)
        self.j_smvPyClient.registerRepoFactory('Python', self.repoFactory)

        # Initialize DataFrame and Column with helper methods
        smv.helpers.init_helpers()

    def appName(self):
        return self.j_smvApp.smvConfig().appName()

    def config(self):
        return self.j_smvApp.smvConfig()

    def appId(self):
        return self.config().appId()

    def discoverSchemaAsSmvSchema(self, path, csvAttributes, n=100000):
        """Discovers the schema of a .csv file and returns a Scala SmvSchema instance

        path --- path to csvfile
        n --- number of records used to discover schema (optional)
        csvAttributes --- Scala CsvAttributes instance (optional)
        """
        return self._jvm.SmvPythonHelper.discoverSchemaAsSmvSchema(path, n, csvAttributes)

    def inputDir(self):
        return self.config().inputDir()

    def getFileNamesByType(self, ftype):
        return self.j_smvApp.getFileNamesByType(self.inputDir(), ftype)

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

    def getModuleResult(self, urn, forceRun = False, version = None):
        """Run module and get its result, which may not be a DataFrame
        """
        fqn = urn[urn.find(":")+1:]
        ds = self.repoFactory.createRepo().loadDataSet(fqn)
        df, collector = self.runModule(urn, forceRun, version)
        return ds.df2result(df)

    def runModule(self, urn, forceRun = False, version = None, runConfig = None):
        """Runs either a Scala or a Python SmvModule by its Fully Qualified Name(fqn)

        Use j_smvPyClient instead of j_smvApp directly so we don't
        have to construct SmvRunCollector from the python side.

        Example:
            To get just the dataframe of the module:
                dataframe = smvApp.runModule('mod:package.module.SmvModuleClass')[0]
            To get both the dataframe and the run info collector:
                dataframe, collector = smvApp.runModule('mod:package.module.SmvModuleClass')

        Returns:
            (DataFrame, SmvRunInfoCollector) tuple
            - DataFrame is the computed result of the module
            - SmvRunInfoCollector contains additional information
              about the run, such as validation results.
        """
        java_result = self.j_smvPyClient.runModule(urn, forceRun, self.scalaOption(version), runConfig)
        return (DataFrame(java_result.df(), self.sqlContext),
                SmvRunInfoCollector(java_result.collector()) )

    def runModuleByName(self, name, forceRun = False, version = None, runConfig = None):
        """Runs a SmvModule by its name (can be partial FQN)

        See the `runModule` method above

        Returns:
            (DataFrame, SmvRunInfoCollector) tuple
            - DataFrame is the computed result of the module
            - SmvRunInfoCollector contains additional information
              about the run, such as validation results.
        """
        java_result = self.j_smvPyClient.runModuleByName(name, forceRun, self.scalaOption(version), runConfig)
        return (DataFrame(java_result.df(), self.sqlContext),
                SmvRunInfoCollector(java_result.collector()) )

    def getMetadataJson(self, urn):
        """Returns the metadata for a given urn"""
        return self.j_smvPyClient.getMetadataJson(urn)

    def inferUrn(self, name):
        return self.j_smvPyClient.inferDS(name).urn().toString()

    def getDsHash(self, name):
        """Get hashOfHash for named module as a hex string
        """
        return self.j_smvPyClient.inferDS(name).verHex()

    def copyToHdfs(self, fileobj, destination):
        """Copies the content of a file object to an HDFS location.

        Args:
            fileobj (file object): a file-like object whose content is to be copied,
                such as one returned by open(), or StringIO
            destination (str): specifies the destination path in the hadoop file system

        The file object is expected to have been opened in binary read mode.

        The file object is closed when this function completes.
        """
        src = FileObjInputStream(fileobj)
        self.j_smvPyClient.copyToHdfs(src, destination)

    def urn2fqn(self, urnOrFqn):
        """Extracts the SMV module FQN portion from its URN; if it's already an FQN return it unchanged
        """
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
