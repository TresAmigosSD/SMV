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
import os.path
import argparse
import uuid
import smv.jprops as jprops

class SmvConfig(object):
    def __init__(self, arglist):
        self.cmdline = self._create_cmdline_conf(arglist)
        self.run_app_flags = _run_app_flags
        
        DEFAULT_SMV_HOME_CONF_FILE = os.getenv('HOME', '') + "/.smv/smv-user-conf.props"

        self.app_dir = self.cmdline.pop('smvAppDir')
        self.static_props = {}
        self.dynamic_props = {}

        self.app_conf_path = self.cmdline.pop('smvAppConfFile')
        self.home_conf_path = DEFAULT_SMV_HOME_CONF_FILE
        self.user_conf_path = self.cmdline.pop('smvUserConfFile')
        self.cmdline_props = dict(self.cmdline.pop('smvProps'))

        self.read_props_from_app_dir(self.cmdline['smvAppDir'])

    def read_props_from_app_dir(self, _app_dir):
        self.app_dir = _app_dir
        self.static_props = self._read_props()

    def merged_props(self):
        res = self.static_props.copy()
        res.update(self.dynamic_props)
        return res

    def print_conf(self):
        print(self.cmdline)
        print(self.merged_props())

    def _create_cmdline_conf(self):
        """Parse arglist to a config dictionary"""
        parser = argparse.ArgumentParser(
            usage="smv-run -m ModuleToRun\n       smv-run --run-app",
            description="For additional usage information, please refer to the user guide and API docs at: \nhttp://tresamigossd.github.io/SMV"
        )

        DEFAULT_SMV_APP_CONF_FILE  = "conf/smv-app-conf.props"
        DEFAULT_SMV_USER_CONF_FILE = "conf/smv-user-conf.props"
    
        parser.add_argument('--cbs-port', dest='cbsPort', type=int, help="python callback server port")

        # Where to find props files
        parser.add_argument('--smv-app-dir', dest='smvAppDir', default=".", help="SMV app directory")
        parser.add_argument('--smv-app-conf', dest='smvAppConfFile', default=DEFAULT_SMV_APP_CONF_FILE, help="app level (static) SMV configuration file path")
        parser.add_argument('--smv-user-conf', dest='smvUserConfFile', default=DEFAULT_SMV_USER_CONF_FILE, help="user level (dynamic) SMV configuration file path")

        # Where to find/store data
        parser.add_argument('--data-dir', dest='dataDir', help="specify the top level data directory")
        parser.add_argument('--input-dir', dest='inputDir', help="specify the input directory (default: datadir/input)")
        parser.add_argument('--output-dir', dest='outputDir', help="specify the output directory (default: datadir/output)")
        parser.add_argument('--hostory-dir', dest='historyDir', help="specify the history directory (default: datadir/history)")
        parser.add_argument('--publish-dir', dest='publishDir', help="specify the publish directory (default: datadir/publish)")

        # All app run flags
        parser.add_argument('--force-run-all', dest='forceRunAll', action="store_true", help="ignore persisted data and force all modules to run")
        parser.add_argument('--publish-jdbc', dest='publishJDBC', action="store_true", help="publish the given modules/stage/app through JDBC connection")
        parser.add_argument('--publish-hive', dest='publishHive', action="store_true", help="publish|export given modules/stage/app to hive tables")
        parser.add_argument('--dead', dest='printDeadModules', action="store_true", help="print a list of the dead modules in this application")
        parser.add_argument('--graph', dest='graph', action="store_true", help="generate a dot dependency graph of the given modules (modules are not run)")
        parser.add_argument('--purge-old-output', dest='purgeOldOutput', action="store_true", help="remove all old output files in output dir")
        parser.add_argument('--dry-run', dest='dryRun', action="store_true", help="determine which modules do not have persisted data and will need to be run")

        # Where to output CSVs
        parser.add_argument('--publish', dest='publish', help="publish the given modules/stage/app as given version")
        parser.add_argument('--export-csv', dest='exportCsv', help="publish|export given modules/stage/app to a CSV file at the given path on the local file system")

        # What modules to run
        parser.add_argument('--run-app', dest='runAllApp', action='store_true', help="run all output modules in all stages in app")
        parser.add_argument('-m', '--run-module', dest='modsToRun', nargs='+', help="run specified list of module FQNs")
        parser.add_argument('-s', '--run-stage', dest='stagesToRun', nargs='+', help="run all output modules in specified stages")

        def parse_props(prop):
            return prop.split("=")

        # command line props override
        parser.add_argument('--smv-props', dest="smvProps", nargs='+', type=parse_props, default=[], help="key=value command line props override")

        res = vars(parser.parse_args(self.arglist))
        app_run_flag_keys = [
            'forceRunAll', 'publishJDBC', 'publishHive', 'printDeadModules', 'graph', 'purgeOldOutput', 'dryRun', 
            'publish', 'exportCsv',
            'runAllApp', 'modsToRun', 'stagesToRun'
        ]
        app_run_flags = {k: res[k] for k in app_run_flag_keys}
        return res


    def _read_props(self):
        # os.path.join has the following behavior:
        # ("/a/b", "c/d") -> "/a/b/c/d"
        # ("/a/b", "/c/d") -> "/c/d"
        full_app_conf_path = os.path.join(self.app_dir, self.app_conf_path)
        full_user_conf_path = os.path.join(self.app_dir, self.user_conf_path)

        with open(full_app_conf_path) as fp:
            app_conf_props = jprops.load_properties(fp)

        with open(self.home_conf_path) as fp:
            home_conf_props = jprops.load_properties(fp)
    
        with open(full_user_conf_path) as fp:
            user_conf_props = jprops.load_properties(fp)

        default_props = {
            "smv.appName"            : "Smv Application",
            "smv.appId"              : str(uuid.uuid4())
            "smv.stages"             : "",
            "smv.config.keys"        : "",
            "smv.class_dir"          : "./target/classes",
            "smv.user_libraries"     : "",
            "smv.maxCbsPortRetries"  : "10"
        }

        res = {}
        res.update(default_props)
        res.update(app_conf_props)
        res.update(home_conf_props)
        res.update(user_conf_props)
        res.update(self.cmdline_props)
        return res