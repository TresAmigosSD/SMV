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
"""
Handle command line args and props files
"""
import argparse

def create_cmdline_conf(arglist):
    """Parse arglist to a config dictionary"""
    parser = argparse.ArgumentParser(
        usage="smv-run -m ModuleToRun\n       smv-run --run-app",
        description="For additional usage information, please refer to the user guide and API docs at: \nhttp://tresamigossd.github.io/SMV"
    )

    DEFAULT_SMV_APP_CONF_FILE  = "conf/smv-app-conf.props"
    DEFAULT_SMV_USER_CONF_FILE = "conf/smv-user-conf.props"
  
    parser.add_argument('--smv-app-dir', dest='smvAppDir', default=".", help="SMV app directory")
    parser.add_argument('--cbs-port', dest='cbsPort', type=int, help="python callback server port")
    parser.add_argument('--smv-app-conf', dest='smvAppConfFile', default=DEFAULT_SMV_APP_CONF_FILE, help="app level (static) SMV configuration file path")
    parser.add_argument('--smv-user-conf', dest='smvUserConfFile', default=DEFAULT_SMV_USER_CONF_FILE, help="user level (dynamic) SMV configuration file path")
    parser.add_argument('--publish', dest='publish', help="publish the given modules/stage/app as given version")
    parser.add_argument('--export-csv', dest='exportCsv', help="publish|export given modules/stage/app to a CSV file at the given path on the local file system")

    parser.add_argument('--force-run-all', dest='forceRunAll', action="store_true", help="ignore persisted data and force all modules to run")
    parser.add_argument('--publish-jdbc', dest='publishJDBC', action="store_true", help="publish the given modules/stage/app through JDBC connection")
    parser.add_argument('--publish-hive', dest='publishHive', action="store_true", help="publish|export given modules/stage/app to hive tables")
    parser.add_argument('--dead', dest='printDeadModules', action="store_true", help="print a list of the dead modules in this application")
    parser.add_argument('--graph', dest='graph', action="store_true", help="generate a dot dependency graph of the given modules (modules are not run)")
    parser.add_argument('--purge-old-output', dest='purgeOldOutput', action="store_true", help="remove all old output files in output dir")
    parser.add_argument('--dry-run', dest='dryRun', action="store_true", help="determine which modules do not have persisted data and will need to be run")

    parser.add_argument('--data-dir', dest='dataDir', help="specify the top level data directory")
    parser.add_argument('--input-dir', dest='inputDir', help="specify the input directory (default: datadir/input)")
    parser.add_argument('--output-dir', dest='outputDir', help="specify the output directory (default: datadir/output)")
    parser.add_argument('--hostory-dir', dest='historyDir', help="specify the history directory (default: datadir/history)")
    parser.add_argument('--publish-dir', dest='publishDir', help="specify the publish directory (default: datadir/publish)")

    parser.add_argument('--run-app', dest='runAllApp', action='store_true', help="run all output modules in all stages in app")
    parser.add_argument('-m', '--run-module', dest='modsToRun', nargs='+', help="run specified list of module FQNs")
    parser.add_argument('-s', '--run-stage', dest='stagesToRun', nargs='+', help="run all output modules in specified stages")

    def parse_props(prop):
        return prop.split("=")

    parser.add_argument('--smv-props', dest="smvProps", nargs='+', type=parse_props, help="key=value command line props override")

    res = vars(parser.parse_args(arglist))
    res.update({'smvProps': dict(res['smvProps'])})

    return res