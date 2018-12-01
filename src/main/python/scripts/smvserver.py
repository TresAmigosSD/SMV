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
import os
import fnmatch
import re
import glob
from flask import Flask, request, jsonify
from pyspark.sql import SparkSession
from smv import SmvApp
from smv.smvapp import DataSetRepoFactory

app = Flask(__name__)

# ---------- Helper Functions ---------- #


def getStagesInApp():
    """returns list of all stages defined in app"""
    return list(SmvApp.getInstance().stages())


def getStageFromFqn(fqn):
    '''returns the stage given a a dataset's fqn'''
    try:
        stage = SmvApp.getInstance().getStageFromModuleFqn(fqn).encode("utf-8")
    except:
        raise ValueError("Could not retrive stage with the given fqn: " + str(fqn))
    return stage

# Indicates whether getDatasetInstance raises an error when trying to load modules
# that do not exist. DataSetRepo.loadDataSet recently changed to raise errors only
# when a module exists but has an error that prevents it from loading. For now,
# external applications that support older versions of SMV need to know which
# behavior to expect.
getDatasetInstance_raises_error_for_dne = False


def getDatasetInstance(fqn):
    '''returns dataset object given a fqn'''
    return DataSetRepoFactory(SmvApp.getInstance()).createRepo().loadDataSet(fqn)


def runModule(fqn, run_config=None):
    '''runs module of given fqn and runtime configuration'''
    return SmvApp.getInstance().runModule(fqn, runConfig=run_config)[0]


def getMetadataHistoryJson(fqn):
    '''returns metadata history given a fqn'''
    return SmvApp.getInstance().getMetadataHistoryJson(fqn)

# Wrapper so that other python scripts can import and then call
# smvserver.Main()
class Main(object):
    def __init__(self):
        options = self.parseArgs()

        # init Smv context
        sparkSession = SparkSession.builder.\
                enableHiveSupport().\
                getOrCreate()

        smvApp = SmvApp.createInstance([], sparkSession)

        # to reduce complexity in SmvApp, keep the rest server single-threaded
        app.run(host=options.ip, port=int(options.port), threaded=False, processes=1)

    def parseArgs(self):
        from optparse import OptionParser
        parser = OptionParser()
        parser.add_option("--port", dest="port", type="int", default=5000,
                  help="smv-server port number [default=5000]")
        parser.add_option("--ip", dest="ip", type="string", default="0.0.0.0",
                  help="smv-server ip to bind to [default=0.0.0.0]")

        (options, args) = parser.parse_args()
        return options

# temporary till module source control is implemented
module_file_map = {}

if __name__ == "__main__":
    Main()
