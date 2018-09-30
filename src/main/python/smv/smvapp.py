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
from datetime import datetime
import os
import sys
import traceback
import json

from py4j.java_gateway import java_import, JavaObject
from pyspark import SparkContext
from pyspark.sql import SparkSession, DataFrame


from smv.datasetmgr import DataSetMgr
from smv.datasetrepo import DataSetRepoFactory
from smv.utils import smv_copy_array, check_socket
from smv.error import SmvRuntimeError, SmvDqmValidationError
import smv.helpers
from smv.utils import FileObjInputStream
from smv.runinfo import SmvRunInfoCollector
from py4j.protocol import Py4JJavaError

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

    # default rel path for python sources from appDir
    SRC_PROJECT_PATH = "src/main/python"
    # default location for py UDL's in smv projects
    SRC_LIB_PATH = "library"

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

    @classmethod
    def setInstance(cls, app):
        """Set the singleton instance.
        """
        cls._instance = app

    def __init__(self, arglist, _sparkSession = None):
        self.sparkSession = SparkSession.builder.\
                    enableHiveSupport().\
                    getOrCreate() if _sparkSession is None else _sparkSession

        #self.prepend_source("src/main/python")

        sc = self.sparkSession.sparkContext
        sc.setLogLevel("ERROR")

        self.sc = sc
        self.sqlContext = self.sparkSession._wrapped
        self._jvm = sc._jvm

        from py4j.java_gateway import java_import
        java_import(self._jvm, "org.tresamigos.smv.ColumnHelper")
        java_import(self._jvm, "org.tresamigos.smv.SmvDFHelper")
        java_import(self._jvm, "org.tresamigos.smv.dqm.*")
        java_import(self._jvm, "org.tresamigos.smv.panel.*")
        java_import(self._jvm, "org.tresamigos.smv.python.SmvPythonHelper")
        java_import(self._jvm, "org.tresamigos.smv.SmvRunInfoCollector")
        java_import(self._jvm, "org.tresamigos.smv.SmvHDFS")
        java_import(self._jvm, "org.tresamigos.smv.URN")

        self.j_smvPyClient = self.create_smv_pyclient(arglist)

        # shortcut is meant for internal use only
        self.j_smvApp = self.j_smvPyClient.j_smvApp()
        self.log = self.j_smvApp.log()
        self.dsm = DataSetMgr(self.sc, self.j_smvApp.dsm())

        # AFTER app is available but BEFORE stages,
        # use the dynamically configured app dir to set the source path, library path
        self.prependDefaultDirs()

        # issue #429 set application name from smv config
        sc._conf.setAppName(self.appName())

        # user may choose a port for the callback server
        gw = sc._gateway
        cbsp = self.j_smvPyClient.callbackServerPort()
        cbs_port = cbsp.get() if cbsp.isDefined() else gw._python_proxy_port

        # check wither the port is in-use or not for several times - if all fail, error out
        check_counter = 0
        while(not check_socket(cbs_port) and check_counter < int(self.maxCbsPortRetries())):
            cbs_port += 1
            check_counter += 1

        if (not check_socket(cbs_port)):
            raise SmvRuntimeError("Start Python callback server failed. Port {0}-{1} are all in use. Please consider increasing the maximum retries or overriding the default port.".format(cbs_port - check_counter, cbs_port))

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

        self.repoFactory = DataSetRepoFactory(self)
        self.dsm.register(self.repoFactory)

        # Initialize DataFrame and Column with helper methods
        smv.helpers.init_helpers()


    def exception_handling(func):
        """ Decorator function to catch Py4JJavaError and raise SmvDqmValidationError if any.
            Otherwise just pass through the original exception
        """
        def func_wrapper(*args, **kwargs):
            try:
                return func(*args, **kwargs)
            except Py4JJavaError as e:
                if (e.java_exception and e.java_exception.getClass().getName() == "org.tresamigos.smv.dqm.SmvDqmValidationError"):
                    dqmValidationResult = json.loads(e.java_exception.getMessage())
                    raise SmvDqmValidationError(dqmValidationResult)
                else:
                    raise e
        return func_wrapper

    def prependDefaultDirs(self):
        """ Ensure that mods in src/main/python and library/ are discoverable.
            If we add more default dirs, we'll make this a set
        """
        self.prepend_source(self.SRC_LIB_PATH)
        self.prepend_source(self.SRC_PROJECT_PATH)

    def removeDefaultDirs(self):
        """ The cleanup version of prependDefaultDirs
        """
        self.remove_source(self.SRC_PROJECT_PATH)
        self.remove_source(self.SRC_LIB_PATH)

    def config(self):
        return self.j_smvApp.smvConfig()

    def appName(self):
        return self.config().appName()

    def maxCbsPortRetries(self):
        return self.config().maxCbsPortRetries()

    def getConf(self, key):
        return self.j_smvPyClient.getRunConfig(key)

    def setAppDir(self, appDir):
        """ SMV's equivalent of 'cd' for app dirs. """
        self.removeDefaultDirs()

        # this call sets the scala side's picture of app dir and forces
        # the app properties to be read from disk and reevaluated
        self.j_smvPyClient.setAppDir(appDir)

        # this call will use the dynamic appDir that we just set ^
        # to change sys.path, allowing py modules, UDL's to be discovered by python
        self.prependDefaultDirs()

    def setDynamicRunConfig(self, runConfig):
        self.j_smvPyClient.setDynamicRunConfig(runConfig)

    def getCurrentProperties(self, raw = False):
        """ Python dict of current megred props
            defaultProps ++ appConfProps ++ homeConfProps ++ usrConfProps ++ cmdLineProps ++ dynamicRunConfig
            Where right wins out in map merge. Pass optional raw param = True to get json string instead
        """
        merged_json = self.j_smvPyClient.mergedPropsJSON()
        return merged_json if raw else json.loads(merged_json)

    def stages(self):
        """Stages is a function as they can be set dynamically on an SmvApp instance"""
        return self.j_smvPyClient.stages()

    def userLibs(self):
        """Return dynamically set smv.user_libraries from conf"""
        return self.j_smvPyClient.userLibs()

    def appId(self):
        return self.config().appId()

    def discoverSchemaAsSmvSchema(self, path, csvAttributes, n=100000):
        """Discovers the schema of a .csv file and returns a Scala SmvSchema instance

        path --- path to csvfile
        n --- number of records used to discover schema (optional)
        csvAttributes --- Scala CsvAttributes instance (optional)
        """
        return self._jvm.SmvPythonHelper.discoverSchemaAsSmvSchema(path, n, csvAttributes)

    def getSchemaByDataFileAsSmvSchema(self, data_file_name):
        """Get the schema of a data file from its path and returns a Scala SmvSchema instance.
           The result will be None if the corresponding schema file does not exist or is invalid.
        """
        data_file_path = os.path.join(self.inputDir(), data_file_name)
        return self.j_smvPyClient.readSchemaFromDataPathAsSmvSchema(data_file_path)

    def inputDir(self):
        return self.config().inputDir()

    def getFileNamesByType(self, ftype):
        all_files = self._jvm.SmvHDFS.dirList(self.inputDir()).array()
        return [str(f) for f in all_files if f.endswith(ftype)]

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

    def getModuleResult(self, urn, forceRun=False, version=None):
        """Run module and get its result, which may not be a DataFrame
        """
        fqn = urn[urn.find(":")+1:]
        ds = self.repoFactory.createRepo().loadDataSet(fqn)
        df, collector = self.runModule(urn, forceRun, version)
        return ds.df2result(df)

    def loadSingleDS(self, urn):
        """Return j_ds from urn"""
        return self.dsm.load(urn)[0]

    @exception_handling
    def runModule(self, urn, forceRun=False, version=None, runConfig=None, quickRun=False):
        """Runs either a Scala or a Python SmvModule by its Fully Qualified Name(fqn)

        Use j_smvPyClient instead of j_smvApp directly so we don't
        have to construct SmvRunCollector from the python side.

        Args:
            urn (str): The URN of a module
            forceRun (bool): True if the module should be forced to run even if it has persisted output. False otherwise.
            version (str): The name of the published version to load from
            runConfig (dict): runtime configuration to use when running the module
            quickRun (bool): skip computing dqm+metadata and persisting csv

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
        self.setDynamicRunConfig(runConfig)
        collector = self._jvm.SmvRunInfoCollector()
        j_ds = self.loadSingleDS(urn)
        if (forceRun):
            self.j_smvPyClient.deleteModuleOutput(j_ds)

        j_df = j_ds.rdd(forceRun, False, collector, quickRun)
        return (DataFrame(j_df, self.sqlContext),
                SmvRunInfoCollector(collector))

    @exception_handling
    def runModuleByName(self, name, forceRun=False, version=None, runConfig=None, quickRun=False):
        """Runs a SmvModule by its name (can be partial FQN)

        See the `runModule` method above

        Args:
            name (str): The unique name of a module. Does not have to be the FQN.
            forceRun (bool): True if the module should be forced to run even if it has persisted output. False otherwise.
            version (str): The name of the published version to load from
            runConfig (dict): runtime configuration to use when running the module
            quickRun (bool): skip computing dqm+metadata and persisting csv

        Returns:
            (DataFrame, SmvRunInfoCollector) tuple
            - DataFrame is the computed result of the module
            - SmvRunInfoCollector contains additional information
              about the run, such as validation results.
        """
        urn = self.dsm.inferUrn(name)
        return self.runModule(urn, forceRun, version, runConfig, quickRun)

    def getRunInfo(self, urn, runConfig=None):
        """Returns the run information of a module and all its dependencies
        from the last run.

        Unlike the runModule() method, which returns the run
        information just for that run, this method returns the run
        information from the last run.

        If no module was run (e.g. the code did not change, so the
        data is read from persistent storage), the SmRunInfoCollector
        returned from the runModule() method would be empty.  But the
        SmvRunInfoCollector returned from this method would contain
        all latest run information about all dependent modules.

        Args:
            urn (str): urn of target module
            runConfig (dict): runConfig to apply when collecting info. If module
                              was run with a config, the same config needs to be
                              specified here to retrieve the info.

        Returns:
            SmvRunInfoCollector

        """
        self.setDynamicRunConfig(runConfig)
        java_result = self.j_smvPyClient.getRunInfo(urn)
        return SmvRunInfoCollector(java_result)

    def getRunInfoByPartialName(self, name, runConfig):
        """Returns the run information of a module and all its dependencies
        from the last run.

        Unlike the runModule() method, which returns the run
        information just for that run, this method returns the run
        information from the last run.

        If no module was run (e.g. the code did not change, so the
        data is read from persistent storage), the SmRunInfoCollector
        returned from the runModule() method would be empty.  But the
        SmvRunInfoCollector returned from this method would contain
        all latest run information about all dependent modules.

        Args:
            name (str): unique suffix to fqn of target module
            runConfig (dict): runConfig to apply when collecting info. If module
                              was run with a config, the same config needs to be
                              specified here to retrieve the info.

        Returns:
            SmvRunInfoCollector
        """
        self.setDynamicRunConfig(runConfig)
        java_result = self.j_smvPyClient.getRunInfoByPartialName(name)
        return SmvRunInfoCollector(java_result)

    @exception_handling
    def publishModuleToHiveByName(self, name, runConfig=None):
        """Publish an SmvModule to Hive by its name (can be partial FQN)
        """
        self.setDynamicRunConfig(runConfig)
        collector = self._jvm.SmvRunInfoCollector()
        return self.dsm.inferDS(name)[0].exportToHive(collector)

    def getMetadataJson(self, urn):
        """Returns the metadata for a given urn"""
        j_ds = self.loadSingleDS(urn)
        return j_ds.getMetadata().toJson()

    def getMetadataHistoryJson(self, urn):
        """Returns the metadata history for a given urn"""
        j_ds = self.loadSingleDS(urn)
        return j_ds.getMetadataHistory().toJson()

    def getDsHash(self, name, runConfig):
        """The current hashOfHash for the named module as a hex string

            Args:
                name (str): The uniquen name of a module. Does not have to be the FQN.
                runConfig (dict): runConfig to apply when collecting info. If module
                                  was run with a config, the same config needs to be
                                  specified here to retrieve the correct hash.

            Returns:
                (str): The hashOfHash of the named module
        """
        self.setDynamicRunConfig(runConfig)
        return self.dsm.inferDS(name)[0].verHex()

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
        self._jvm.SmvHDFS.writeToFile(src, destination)

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
        return DataFrame(self.j_smvApp.createDF(schema, data), self.sqlContext)

    def _mkCsvAttr(self, delimiter=',', quotechar='"', hasHeader=False):
        """Factory method for creating instances of Scala case class CsvAttributes"""
        return self._jvm.org.tresamigos.smv.CsvAttributes(delimiter, quotechar, hasHeader)

    def defaultCsvWithHeader(self):
        return self._mkCsvAttr(hasHeader=True)

    def defaultTsv(self):
        return self._mkCsvAttr(delimiter='\t')

    def defaultTsvWithHeader(self):
        return self._mkCsvAttr(delimier='\t', hasHeader=True)

    def abs_path_for_project_path(self, project_path):
        # Load dynamic app dir from scala
        smvAppDir = self.config().appDir()
        return os.path.abspath(os.path.join(smvAppDir, project_path))

    def prepend_source(self, project_path):
        abs_path = self.abs_path_for_project_path(project_path)
        # Source must be added to front of path to make sure it is found first
        sys.path.insert(1, abs_path)
        self.log.debug("Prepended {} to sys.path".format(abs_path))

    def remove_source(self, project_path):
        try:
            abs_path = self.abs_path_for_project_path(project_path)
            sys.path.remove(abs_path)
            self.log.debug("Removed {} from sys.path".format(abs_path))
        except ValueError:
            # ValueError will be raised if the project path was not previously
            # added to the sys.path
            pass

    def run(self):
        self.j_smvApp.run()
