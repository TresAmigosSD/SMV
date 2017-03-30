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
import json
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
        'class (.+?)\(SmvPyModule',
        'class (.+?)\(SmvPyCsvFile',
    ]
    module_dict = get_module_file_mapping(files, patterns)
    return module_dict

# ---------- API Definition ---------- #

MODULE_NOT_PROVIDED_ERR = 'ERROR: No module name is provided!'
MODULE_NOT_FOUND_ERR = 'ERROR: Job failed to run. Please check whether the module name is valid!'
MODULE_ALREADY_EXISTS_ERR = 'ERROR: Module already exists!'
TYPE_NOT_PROVIDED_ERR = 'ERROR: No module type is provided!'
TYPE_NOT_SUPPORTED_ERR = 'ERROR: Module type not supported!'
CODE_NOT_PROVIDED_ERR = 'ERROR: No module code is provided!'
JOB_SUCCESS = 'SUCCESS: Job finished.'

@app.route("/api/run_module", methods = ['POST'])
def run_module():
    '''
    body: name = 'xxx' (fqn)
    function: run the module
    '''
    try:
        module_name = request.form['name']
    except:
        raise ValueError(MODULE_NOT_PROVIDED_ERR)

    try:
        smvPy.runModule(module_name.strip())
        return JOB_SUCCESS
    except:
        raise ValueError(MODULE_NOT_FOUND_ERR)

@app.route("/api/get_module_code", methods = ['POST'])
def get_module_code():
    '''
    body: name = 'xxx' (fqn)
    function: return the module's code
    '''
    try:
        module_name = request.form['name']
    except:
        raise ValueError(MODULE_NOT_PROVIDED_ERR)

    try:
        global module_file_map
        if not module_file_map:
            module_file_map = get_module_code_file_mapping()
        file_name = module_file_map[module_name]
        with open(file_name, 'rb') as f:
            res = f.readlines()
        file_content = [line.rstrip() for line in res]
        res = {
            'fileName': file_name,
            'fileContent': file_content,
        }
        return jsonify(res=res)
    except:
        raise ValueError(MODULE_NOT_FOUND_ERR)

@app.route("/api/update_module_code", methods = ['POST'])
def update_module_code():
    '''
    body:
        name = 'xxx' (fqn)
        code = ['line1', 'line2', ...]
    function: update the module's code
    '''
    try:
        module_name = request.form['name']
    except:
        raise ValueError(MODULE_NOT_PROVIDED_ERR)

    try:
        module_code = request.form['code']
    except:
        raise ValueError(CODE_NOT_PROVIDED_ERR)

    global module_file_map
    if not module_file_map:
        module_file_map = get_module_code_file_mapping()

    if (module_file_map.has_key(module_name)):
        file_name = module_file_map[module_name]
        module_code = json.loads(module_code)
        module_code = [line.encode('utf-8') for line in module_code]
        with open(file_name, 'wb') as f:
            f.writelines(os.linesep.join(module_code))
        return JOB_SUCCESS
    else:
        # TODO: deal with new module
        raise ValueError(MODULE_NOT_FOUND_ERR)

@app.route("/api/get_sample_output", methods = ['POST'])
def get_sample_output():
    '''
    body: name = 'xxx' (fqn)
    function: return the module's sample output
    '''
    try:
        module_name = request.form['name']
    except:
        raise ValueError(MODULE_NOT_PROVIDED_ERR)

    try:
        output_dir = get_output_dir()
        latest_dir = get_latest_file_dir(output_dir, module_name, '.csv')
        res = read_file_dir(latest_dir, limit=20)
        return jsonify(res=res)
    except:
        raise ValueError(MODULE_NOT_FOUND_ERR)

@app.route("/api/get_module_schema", methods = ['POST'])
def get_module_schema():
    '''
    body: name = 'xxx' (fqn)
    function: return the module's schema
    '''
    try:
        module_name = request.form['name']
    except:
        raise ValueError(MODULE_NOT_PROVIDED_ERR)

    try:
        output_dir = get_output_dir()
        latest_dir = get_latest_file_dir(output_dir, module_name, '.schema')
        res = read_file_dir(latest_dir)
        return jsonify(res=res)
    except:
        raise ValueError(MODULE_NOT_FOUND_ERR)

@app.route("/api/get_graph_json", methods = ['POST'])
def get_graph_json():
    '''
    body: none
    function: return the json file of the entire dependency graph
    '''
    res = smvPy.get_graph_json()
    return jsonify(res=res)

@app.route("/api/create_module", methods = ['POST'])
def craete_module():
    '''
    body:
        name = 'xxx' (fqn)
        type = 'xxx' ('scala'/'python'/'sql')
    function:
        1. create code file (if not exists)
        2. add module into module_file_map
    '''
    try:
        module_name = request.form['name'].encode('utf-8')
    except:
        raise ValueError(MODULE_NOT_PROVIDED_ERR)

    try:
        module_type = request.form['type'].encode('utf-8')
    except:
        raise ValueError(TYPE_NOT_PROVIDED_ERR)

    # get module_name and module_code_file mapping
    global module_file_map
    if not module_file_map:
        module_file_map = get_module_code_file_mapping()

    # if module_name already exists, then it is not a new module
    if (module_file_map.has_key(module_name)):
        raise ValueError(MODULE_ALREADY_EXISTS_ERR)

    # get module_code_file for the given module_name
    if (module_type == 'python'):
        main_path = os.getcwd() + '/src/main/python/'
        file_path = '/'.join(module_name.split('.')[:-1])
        suffix = '.py'
    elif (module_type == 'scala'):
        main_path = os.getcwd() + '/src/main/scala/'
        file_path = '/'.join(module_name.split('.'))
        suffix = '.scala'
    else:
        raise ValueError(TYPE_NOT_SUPPORTED_ERR)

    file_full_path = main_path + file_path + suffix

    # create an empty file if the module_code_file not exists
    open(file_full_path, 'a').close()

    # add the new module into module_name and module_code_file mapping
    module_file_map[module_name] = file_full_path

    res = {
        'moduleName': module_name,
    }
    return jsonify(res=res)


if __name__ == "__main__":
    # TODO: should be done by SmvApp (python) automatically.
    codePath = os.path.abspath("src/main/python")
    sys.path.insert(1, codePath)

    # init Smv context
    # TODO: should instantiate SmvApp here instead of callint init directly.
    smvPy.init([])
    module_file_map = {}

    # start server
    host = os.environ.get('SMV_HOST', '0.0.0.0')
    port = os.environ.get('SMV_PORT', '5000')
    app.run(host=host, port=int(port))
