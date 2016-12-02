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

# TODO: why are we compiling the files?
def compile_python_files(path):
    r = compileall.compile_dir(path, quiet=1)
    if not r:
        exit(-1)

# TODO: this is not needed on the restful server.
def print_module_names(mods):
    print("----------------------------------------")
    print("will run the following modules:")
    for name in mods:
        print("   " + name)
    print("----------------------------------------")

def get_output_dir():
    output_dir = smvPy.outputDir()
    if (output_dir.startswith('file://')):
        output_dir = output_dir[7:]
    return output_dir

# TODO: need to document this function.  Explain why we want the latest and give an example
# of how there would multiple data file dirs with same name/suffix.
def get_latest_file_dir(output_dir, module_name, suffix):
    latest_file_dir = max([f for f in os.listdir(output_dir) \
        if f.startswith(module_name) and f.endswith(suffix)], \
        key=lambda f: os.path.getctime(os.path.join(output_dir, f)))
    return os.path.join(output_dir, latest_file_dir)

# TODO: explain what a file dir is and what it consists of.
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
                        # TODO: should probably compile the set of patterns once at beginning of this function.   This is a very expensive step and should only be done once and not multiple times per line per file.
                        m = re.search(pattern, line)
                        if m:
                            module_name = m.group(1).strip()
                            file_name = file
                            fqn = get_fqn(module_name, file_name)
                            if (fqn):
                                module_dict[fqn] = file_name
        return module_dict

# TODO: document this and how it works (not very obvious from code)
    def get_fqn(module_name, file_name):
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

# TODO: entrpy point should be /run_module (singular) and only accept a single module name (note the current mix of plurality of module_names vs. name).  The rest of the code is acting on a single module so we will be consistent here and only run one module at a time.
# 
@app.route("/run_modules", methods = ['GET'])
def run_modules():
    '''
    Take FQNs as parameter and run the modules
    '''
    module_names = request.args.get('name', '')
    if module_names:
        module_names = module_names.strip().split()
        print_module_names(module_names)
        for fqn in module_names:
            smvPy.runModule(fqn)
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
