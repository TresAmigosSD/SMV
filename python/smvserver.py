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

def compile_python_files(path):
    r = compileall.compile_dir(path, quiet=1)
    if not r:
        exit(-1)

def print_module_names(mods):
    print("----------------------------------------")
    print("will run the following modules:")
    for name in mods:
        print("   " + name)
    print("----------------------------------------")

def get_output_dir():
    output_dir = smvPy.outputDir()
    if(output_dir.startswith('file://')):
        output_dir = output_dir[7:]
    return output_dir

def get_latest_file_dir(output_dir, module_name, suffix):
    latest_file_dir = max([f for f in os.listdir(output_dir) \
        if f.startswith(module_name) and f.endswith(suffix)], \
        key=lambda f: os.path.getctime(os.path.join(output_dir, f)))
    return os.path.join(output_dir, latest_file_dir)

def read_file_dir(file_dir, limit=999999999):
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
                        m = re.search(pattern, line)
                        if m:
                            module_name = m.group(1).strip()
                            file_name = file
                            fqn = get_fqn(module_name, file_name)
                            if (fqn):
                                module_dict[fqn] = file_name
        return module_dict

    def get_fqn(module_name, file_name):
        sep = os.path.sep
        file_name_split = file_name.strip().split(sep)

        # TODO there are also other top-level package (e.g. org)
        # need to figure out a more general way to find the modules
        try:
            start_index = file_name_split.index('com')
        except ValueError:
            return None
        else:
            if file_name_split[-1].endswith('.scala'):
                file_name_split.pop()
            elif file_name_split[-1].endswith('.py'):
                file_name_split[-1] = file_name_split[-1][:-3]
            fqn_split = file_name_split[start_index:]
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

@app.route("/run_modules", methods = ['GET'])
def run_modules():
    '''
    Take FQNs as parameter and run the modules
    '''
    module_name = request.args.get('name', 'NA')
    # create SMV app instance
    arglist = ['-m'] + module_name.strip().split()
    smvPy.create_smv_pyclient(arglist)
    # run modules
    modules = smvPy.moduleNames()
    print_module_names(modules)
    publish = smvPy.isPublish()
    for name in modules:
        if publish:
            smvPy.publishModule(name)
        else:
            smvPy.runModule(name)
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
    compile_python_files('src/main/python')

    # init Smv context
    smvPy.init([])
    module_file_map = {}

    # start server
    app.run(host='0.0.0.0')
