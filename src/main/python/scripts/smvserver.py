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
import json
from flask import Flask, request, jsonify
from smv import SmvApp
from smv.smvapp import DataSetRepoFactory
from shutil import copyfile
import py_compile
import json
from smv.smvdataset import SmvCsvFile
from smv.smvdataset import SmvHiveTable
import ast
import errno

app = Flask(__name__)

# ---------- Helper Functions ---------- #

def getStagesInApp():
    """returns list of all stages defined in app"""
    return list(SmvApp.getInstance().stages)

def getFqnsInApp():
    """returns all known module FQNs in app. Note: excluded links"""
    repo = DataSetRepoFactory(SmvApp.getInstance()).createRepo()
    # generate list of URNs in a stage for each stage (list-of-list)
    urnsLL = [repo.dataSetsForStage(s) for s in getStagesInApp()]
    # flatten the list-of-list to simple list of urns and remove the "mod:" prefix
    urns = [u.split(":")[1] for ul in urnsLL for u in ul]
    return urns

def get_filepath_from_moduleFqn(module_fqn):
    '''given a fqn, returns the fullname of its file relative to proj dir.'''
    prefix = "./src/main/python/"
    # dir1.dir2.file.class => [dir1, dir2, file]
    fqn_dirs_filename_list = module_fqn.split(".")[:-1]
    # concats fqn_dirs_filename_list into string with "/" intermezzo, appends .py, prepends prefix
    filepath = prefix + "/".join(fqn_dirs_filename_list) + ".py"
    return filepath

def get_output_dir():
    '''returns the smv app's output directory'''
    output_dir = SmvApp.getInstance().outputDir()
    if (output_dir.startswith('file://')):
        output_dir = output_dir[7:]
    return output_dir

def get_latest_file_dir(output_dir, module_name, suffix):
    '''
    There could be multiple output snapshots for a module in the output dir, like:
        com.mycompany.myproj.stage1.employment.PythonEmploymentByState_13e82a9b.csv
        com.mycompany.myproj.stage1.employment.PythonEmploymentByState_51d96a9b.csv
        ...
    This function is to find the latest output version for certain module.
    '''
    latest_file_dir = max([f for f in os.listdir(output_dir) \
        if f.startswith(module_name) and f.endswith(suffix)], \
        key=lambda f: os.path.getctime(os.path.join(output_dir, f)))
    return os.path.join(output_dir, latest_file_dir)

def read_file_dir(file_dir, limit=999999999):
    '''
    "file_dir" is the data/schema path of SMV output. The SMV output is split into
    multiple parts and stored under "file_dir" like part-00001, part-00002...
    This function is to read lines of all the files and return them as a list.
    User can use the "limit" parameter to return the first n lines.
    '''
    lines = []
    for file in glob.glob('%s/part-*' % file_dir):
        with open(file, 'rb') as readfile:
            for line in readfile.readlines():
                lines.append(line.rstrip())
                if len(lines) >= limit:
                    break
        if len(lines) >= limit:
            break
    return lines

def get_module_code_file_mapping():
    '''
    This function returns a dictionary where the key is the module name and the
    value is the absolute file path of this module.
    '''
    def get_all_files_with_suffix(path, suffix):
        '''
        This function recurseively searches for all the files with certain
        suffix under certain path and return the absolute file names in a list.
        '''
        matches = []
        for root, dirnames, filenames in os.walk(path):
            for filename in fnmatch.filter(filenames, '*.%s' % suffix):
                matches.append(os.path.join(root, filename))
        return matches

    def get_module_file_mapping(files, patterns):
        module_dict = {}
        # compile the patterns
        patterns = [re.compile(pattern) for pattern in patterns]
        for file in files:
            with open(file, 'rb') as readfile:
                for line in readfile.readlines():
                    # in Python 3, readlines() return a bytes-like object
                    if sys.version >= '3': line = line.decode()
                    for pattern in patterns:
                        m = pattern.search(line)
                        if m:
                            module_name = m.group(1).strip()
                            file_name = file
                            fqn = get_fqn(module_name, file_name)
                            if (fqn):
                                module_dict[fqn] = file_name
        return module_dict

    def get_fqn(module_name, file_name):
        '''
        "module_name" is like "EmploymentByState".
        "file_name" is the absolute file path containing this module, like
            "/xxx/xxx/MyApp/src/main/scala/com/mycompany/myproj/stage1/EmploymentByState.scala".
        This function will return the fqn of the module, like
            "com.mycompany.myproj.stage1.EmploymentByState".
        It will work for both scala and python modules.
        '''
        sep = os.path.sep
        patterns = [
            '(.+?)%s(.+?)$' % sep.join(['src', 'main', 'scala', '']),
            '(.+?)%s(.+?)$' % sep.join(['src', 'main', 'python', '']),
        ]
        for pattern in patterns:
            m = re.search(pattern, file_name)
            if m:
                fqn_split = m.group(2).strip().split(sep)
                if fqn_split[-1].endswith('.scala'):
                    fqn_split.pop()
                elif fqn_split[-1].endswith('.py'):
                    fqn_split[-1] = fqn_split[-1][:-3]
                fqn_split.append(module_name)
                fqn = '.'.join(fqn_split)
                return fqn

    code_dir = os.getcwd() + '/src'
    scala_files = get_all_files_with_suffix(code_dir, 'scala')
    python_files = get_all_files_with_suffix(code_dir, 'py')

    files = scala_files + python_files
    patterns = [
        'object (.+?) extends( )+SmvModule\(',
        'object (.+?) extends( )+SmvCsvFile\(',
        'class (.+?)\(SmvModule',
        'class (.+?)\(SmvCsvFile',
    ]
    module_dict = get_module_file_mapping(files, patterns)
    return module_dict

def getStageFromFqn(fqn):
    '''returns the stage given a a dataset's fqn'''
    # constructing urn for dataset
    try:
        stage = SmvApp.getInstance().getStageFromModuleFqn(fqn).encode("utf-8")
    except:
        raise ValueError("Could not retrive stage with the given fqn: " + str(fqn))
    return stage

def getDatasetInstance(fqn):
    return DataSetRepoFactory(SmvApp.getInstance()).createRepo().loadDataSet(fqn)

def runModule(fqn):
    return SmvApp.getInstance().runModule("mod:{}".format(fqn))

# ---------- API Definition ---------- #

@app.route("/api/run_module", methods = ['POST'])
def run_module():
    '''
    body: fqn = 'xxx' (fqn)
    function: run the module
    '''
    try:
        module_fqn = request.form['fqn'].encode("utf-8")
    except:
        raise err_res('MODULE_NOT_PROVIDED_ERR')
    return ok_res(str(runModule(module_fqn)))

@app.route("/api/get_graph_json", methods = ['POST'])
def get_graph_json():
    '''
    body: none
    function: return the json file of the entire dependency graph
    '''
    res = SmvApp.getInstance().get_graph_json()
    return jsonify(graph=res)


# Wrapper so that other python scripts can import and then call
# smvserver.Main()
class Main(object):
    def __init__(self):
        options = self.parseArgs()

        # init Smv context
        smvApp = SmvApp.createInstance([])

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
