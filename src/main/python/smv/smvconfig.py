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
import re
import argparse
import uuid
import smv.jprops as jprops
from smv.error import SmvRuntimeError

class SmvConfig(object):
    """Smv configurations 
        Including:
            - command line parsing
            - read in property files
            - dynamic configuration handling
    """
    def __init__(self, arglist, _jvm):
        self._jvm = _jvm
        self.cmdline = self._create_cmdline_conf(arglist)
        
        DEFAULT_SMV_HOME_CONF_FILE = os.getenv('HOME', '') + "/.smv/smv-user-conf.props"

        self.app_dir = self.cmdline.pop('smvAppDir')
        self.static_props = {}
        self.dynamic_props = {}

        self.app_conf_path = self.cmdline.pop('smvAppConfFile')
        self.home_conf_path = DEFAULT_SMV_HOME_CONF_FILE
        self.user_conf_path = self.cmdline.pop('smvUserConfFile')
        self.cmdline_props = dict(self.cmdline.pop('smvProps'))

        self.read_props_from_app_dir(self.app_dir)

        self.mods_to_run = self.cmdline.pop('modsToRun')
        self.stages_to_run = self.cmdline.pop('stagesToRun')

        from py4j.java_gateway import java_import
        java_import(self._jvm, "org.tresamigos.smv.SmvConfig")

        # Send conf result to Scala side
        self.j_smvconf = self._jvm.SmvConfig(
            self.cmdline.get('genEdd'),
            self.merged_props(), 
            self.all_data_dirs()
        )

    def reset_j_smvconf(self):
        """Reset scala side conf - for dynamic conf
        """
        self.j_smvconf.reset(self.merged_props(), self.all_data_dirs())

    def read_props_from_app_dir(self, _app_dir):
        """For a given app dir, read in the prop files
        """
        self.app_dir = _app_dir
        self.static_props = self._read_props()

    def merged_props(self):
        """All the props (static + dynamic)
        """
        res = self.static_props.copy()
        res.update(self.dynamic_props)
        return res

    def spark_sql_props(self):
        return {k:v 
            for k, v in self.merged_props().items()
            if k.startswith("spark.sql")
        }

    def set_dynamic_props(self, new_d_props):
        """Reset dynamic props
            Overwrite entire dynamic props fully each reset
            Ignore reset if new_d_props is None
        """
        if(new_d_props is not None):
            self.dynamic_props = new_d_props.copy()
            self.reset_j_smvconf()

    def set_app_dir(self, new_app_dir):
        """Dynamic reset of app dir, so that the location of app and user
            conf files. Re-read the props files
        """
        if(new_app_dir):
            self.app_dir = new_app_dir
            self.read_props_from_app_dir(self.app_dir)
            self.reset_j_smvconf()

    def all_data_dirs(self):
        """Create all the data dir configs
        """
        props = self.merged_props()
        if (self.cmdline.get('dataDir')):
            data_dir = self.cmdline.get('dataDir')
        elif (props.get('smv.dataDir')):
            data_dir = props.get('smv.dataDir')
        elif(os.getenv('DATA_DIR', None)):
            data_dir = os.getenv('DATA_DIR')
            print("WARNING: use of DATA_DIR environment variable is deprecated. use smv.dataDir instead!!!")
        else:
            raise SmvRuntimeError("Must specify a data-dir either on command line or in conf.")

        def get_sub_dir(name, default):
            res = "{}/{}".format(data_dir, default)
            if (self.cmdline.get(name)):
                res = self.cmdline.get(name)
            elif (props.get('smv.' + name)):
                res = props.get('smv.' + name)
            return res

        return {
            'dataDir': data_dir,
            'inputDir': get_sub_dir('inputDir', "input"),
            'outputDir': get_sub_dir('outputDir', "output"),
            'historyDir': get_sub_dir('historyDir', "history"),
            'publishDir': get_sub_dir('publishDir', 'publish'),
            'publishVersion': self.cmdline.get('publish')
        }

    def app_id(self):
        return self.merged_props().get("smv.appId")
    
    def app_name(self):
        return self.merged_props().get("smv.appName")
    
    def user_libs(self):
        return self._split_prop("smv.user_libraries")

    def stage_names(self):
        return self._split_prop("smv.stages")

    def get_run_config(self, key):
        """Run config will be accessed within client modules. Return 
            run-config value of the given key.

            2 possible sources of run-config:
                - dynamic_props (which passed in by client code)
                - props files/command-line parameters
        """
        if (key in self.dynamic_props):
            # when seting run-config in dynamic props, use the key directly
            return self.dynamic_props.get(key).strip()
        else:
            # when seting run-config in props, use smv.config.+key as key
            return self.merged_props().get("smv.config." + key, None)

    def get_run_config_keys(self):
        """Return all the run-config keys
        """
        pref = "smv.config."
        pref_len = len(pref)
        from_props = [k[pref_len:] for k in self.merged_props().keys() if k.startswith(pref)]
        from_dynamic = self.dynamic_props.keys()
        # dict_keys objects are not add-able, have to copy them to a new list
        res = []
        res.extend(from_props)
        res.extend(from_dynamic)
        return res

    def infer_stage_full_name(self, part_name):
        """For a given partial stage name, infer full stage name
        """
        all_stages = self.stage_names()
        candidates = [s for s in all_stages if s.endswith(part_name)]

        if (len(candidates) == 0):
            raise SmvRuntimeError("Can't find stage {}".format(part_name))
        elif(len(candidates) == 1):
            return candidates[0]
        else:
            raise SmvRuntimeError("Stage name {} is ambiguous".format(part_name))


    def _split_prop(self, prop_name):
        """Split multi-value prop to a list
        """
        prop_val = self.merged_props().get(prop_name)
        return [f.strip() for f in re.split("[:,]", prop_val)]

    def _create_cmdline_conf(self, arglist):
        """Parse arglist to a config dictionary
        """
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
        parser.add_argument('--edd', dest='genEdd', action="store_true", help="summarize data and generate an edd file in the same directory as csv and schema")

        # Where to output CSVs
        parser.add_argument('--publish', dest='publish', help="publish the given modules/stage/app as given version")
        parser.add_argument('--export-csv', dest='exportCsv', help="publish|export given modules/stage/app to a CSV file at the given path on the local file system")

        # What modules to run
        parser.add_argument('--run-app', dest='runAllApp', action='store_true', help="run all output modules in all stages in app")
        parser.add_argument('-m', '--run-module', dest='modsToRun', nargs='+', default=[], help="run specified list of module FQNs")
        parser.add_argument('-s', '--run-stage', dest='stagesToRun', nargs='+', default=[], help="run all output modules in specified stages")

        def parse_props(prop):
            # str.split([sep[, maxsplit]]): we just need to split the first "="
            return prop.split("=", 1) 

        # command line props override
        parser.add_argument('--smv-props', dest="smvProps", nargs='+', type=parse_props, default=[], help="key=value command line props override")

        res = vars(parser.parse_args(arglist))
        return res


    def _read_props(self):
        """Read property files
        """
        # os.path.join has the following behavior:
        # ("/a/b", "c/d") -> "/a/b/c/d"
        # ("/a/b", "/c/d") -> "/c/d"
        full_app_conf_path = os.path.join(self.app_dir, self.app_conf_path)
        full_user_conf_path = os.path.join(self.app_dir, self.user_conf_path)

        def load(path):
            if os.path.exists(path):
                with open(path) as fp:
                    return jprops.load_properties(fp)
            else: 
                return {}

        app_conf_props = load(full_app_conf_path)
        home_conf_props = load(self.home_conf_path)
        user_conf_props = load(full_user_conf_path)

        default_props = {
            "smv.appName"            : "Smv Application",
            "smv.appId"              : str(uuid.uuid4()),
            "smv.stages"             : "",
            "smv.config.keys"        : "",
            "smv.class_dir"          : "./target/classes",
            "smv.user_libraries"     : "",
            "smv.maxCbsPortRetries"  : "10"
        }

        # Priority: Low to High
        #   - default 
        #   - conf/smv-app-conf.props
        #   - ${HOME}/.smv/smv-user-conf.props
        #   - conf/smv-user-conf.props
        #   - command-line
        res = {}
        res.update(default_props)
        res.update(app_conf_props)
        res.update(home_conf_props)
        res.update(user_conf_props)
        res.update(self.cmdline_props)
        return res