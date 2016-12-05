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
import os
import fnmatch
import re
import glob
from flask import Flask, request, jsonify
from smv import smvPy
import compileall

app = Flask(__name__)

# ---------- Helper Functions ---------- #

def get_output_dir():
    output_dir = smvPy.outputDir()
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
                lines.append(line)
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
        for file in files:
            with open(file, 'rb') as readfile:
                for line in readfile.readlines():
                    # in Python 3, readlines() return a bytes-like object
                    if sys.version >= '3': line = line.decode()
                    for pattern in patterns:
                        # TODO: should probably compile the set of patterns once at beginning of this function.   This is a very expensive step and should only be done once and not multiple times per line per file.
                        m = re.search(pattern, line)
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
        'class (.+?)\(SmvPyModule',
        'class (.+?)\(SmvPyCsvFile',
    ]
    module_dict = get_module_file_mapping(files, patterns)
    return module_dict

# ---------- API Definition ---------- #

# TODO: each entry point should have a comment with a description of the required parameters.
# TODO: need to handle errors better.  What if the "name" is not specified, or module does not exit, etc.

@app.route("/run_module", methods = ['GET'])
def run_module():
    '''
    Take FQN as parameter and run the module
    '''
    module_name = request.args.get('name', '')
    if module_name:
        smvPy.runModule(module_name.strip())
    return ''

@app.route("/get_module_code", methods = ['GET'])
def get_module_code():
    '''
    Take FQN as parameter and return the module code
    '''
    module_name = request.args.get('name', 'NA')
    global module_file_map
    if not module_file_map:
        module_file_map = get_module_code_file_mapping()
    file_name = module_file_map[module_name]
    with open(file_name, 'rb') as f:
        res = f.readlines()
    return jsonify(res=res)

@app.route("/get_sample_output", methods = ['GET'])
def get_sample_output():
    '''
    Take FQN as parameter and return the sample output
    '''
    module_name = request.args.get('name', 'NA')
    output_dir = get_output_dir()
    latest_dir = get_latest_file_dir(output_dir, module_name, '.csv')
    res = read_file_dir(latest_dir, limit=20)
    return jsonify(res=res)

@app.route("/get_module_schema", methods = ['GET'])
def get_module_schema():
    '''
    Take FQN as parameter and return the module schema
    '''
    module_name = request.args.get('name', 'NA')
    output_dir = get_output_dir()
    latest_dir = get_latest_file_dir(output_dir, module_name, '.schema')
    res = read_file_dir(latest_dir)
    return jsonify(res=res)

@app.route("/get_graph_json", methods = ['GET'])
def get_graph_json():
    res = smvPy.get_graph_json()
    return jsonify(res=res)



if __name__ == "__main__":
    # init Smv context
    smvPy.init([])
    module_file_map = {}

    # start server
    app.run(host='0.0.0.0')
