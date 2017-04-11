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
from smv import SmvApp
from shutil import copyfile
import py_compile

app = Flask(__name__)

TAB_SIZE = 4

# ---------- Helper Functions ---------- #

def indentation(tabbed_str):
    no_tabs_str = tabbed_str.expandtabs(TAB_SIZE)
    # if string has only whitespace, return 0 indentation.
    # else return length of string minus len of str with preceding whitespace stripped
    return 0 if no_tabs_str.isspace() else len(no_tabs_str) - len(no_tabs_str.lstrip())

def test_compile_for_errors(fullname):
    error = None
    try:
        ok = py_compile.compile(fullname, None, None, True)
    except py_compile.PyCompileError,err:
        print 'Compiling', fullname, '...'
        print err.msg
        error = err.msg
    except IOError, e:
        error = e
    else:
        if ok == 0:
            error = 1

    return error

# def get_SMV_module_run_method_start_end(lines_of_code_list):
#     test_module_class_def = "class EmploymentByState(SmvModule"
#     module_definition_found = False
#     run_method_start = None
#     run_method_end = None
#     # detect module class def
#     for i, line in enumerate(lines_of_code_list):
#         if line.lstrip().startswith(test_module_class_def):
#             module_definition_found = True
#             test_run_method = (" " * (indentation(line) + TAB_SIZE)) + "def run("
#         # detect run method if not already found
#         if (module_definition_found and run_method_start is None) \
#         and line.expandtabs(TAB_SIZE).startswith(test_run_method):
#             run_method_start = i
#             continue
#         # detect end of run method
#         if run_method_start is not None:
#             if (run_method_end is None and i == (len(lines_of_code_list) - 1)):
#                 run_method_end = i;
#     return (run_method_start, run_method_end)

# assumes class definition indentation is always 0
def get_SMV_module_class_start_end(lines_of_code_list, module_name):
    test_module_class_def = "class {}(".format(module_name)
    start = None
    end = None

    # detect module class def
    for i, line in enumerate(lines_of_code_list):
        if line.lstrip().startswith(test_module_class_def):
            start = i
            continue
        if start is not None:
            # check if string not blank and has no indentation, or this is the last line
            if (not line.isspace() and indentation(line) == 0):
                end = i
                break
    if start is not None and end is None:
        end = len(lines_of_code_list)
    return (start, end)


def get_filepath_from_moduleFqn(module_fqn):
    # TODO: what env var for projects/ directory?
    prefix = '/projects/' + project_dir + "/src/main/python/"
    # dir1.dir2.file.class => [dir1, dir2, file]
    fqn_dirs_filename_list = module_fqn.split(".")[:-1]
    # concats fqn_dirs_filename_list into string with "/" intermezzo, appends .py, prepends prefix
    filepath = prefix + "/".join(fqn_dirs_filename_list) + ".py"
    return filepath

def get_output_dir():
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

def err_res(err, err_msg="", res={}): # err, errmsg="", res={}
    retval = {}
    retval["err"] = err
    retval["err_msg"] = err_msg
    retval["res"] = res
    return jsonify(retval)

def ok_res(res):
    retval = {}
    retval["err"] = OK
    retval["res"] = res
    return jsonify(retval)

def get_module_name_from_fqn(fqn):
    return fqn.split(".")[-1:][0]   # a.b.c => [a,b,c] => [c] => c

# ---------- API Definition ---------- #

MODULE_NOT_PROVIDED_ERR = 'ERROR: No module name provided!'
MODULE_NOT_FOUND_ERR = 'ERROR: Job failed to run. Please check whether the module name is valid!'
MODULE_ALREADY_EXISTS_ERR = 'ERROR: Module already exists!'
TYPE_NOT_PROVIDED_ERR = 'ERROR: No module type provided!'
TYPE_NOT_SUPPORTED_ERR = 'ERROR: Module type not supported!'
CODE_NOT_PROVIDED_ERR = 'ERROR: No module code provided!'
COMPILATION_ERROR = 'ERROR: Compilation error'
JOB_SUCCESS = 'SUCCESS: Code updated!'
OK = ""

@app.route("/api/test", methods = ['GET'])
def get_project_dir():
    return ok_res('success msg')

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

    run_result = SmvApp.getInstance().runModule("mod:{}".format(module_fqn))
    return ok_res(str(run_result))

@app.route("/api/get_module_code", methods = ['POST'])
def get_module_code():
    '''
    body: fqn = 'xxx'
    function: return the module's code
    '''
    try:
        module_fqn = request.form['fqn'].encode("utf-8")
    except:
        return err_res(MODULE_NOT_PROVIDED_ERR)

    file_name = get_filepath_from_moduleFqn(module_fqn)
    module_name = get_module_name_from_fqn(module_fqn)

    try:
        with open(file_name, 'rb') as f:
            lines_of_code_list = f.readlines()
        (class_start, class_end) = get_SMV_module_class_start_end(lines_of_code_list, module_name)

        file_content = [line.rstrip() for line in lines_of_code_list]

        res = {
            'fileName': file_name,
            'fileContent': file_content # file_content[class_start:class_end],
        }
        return ok_res(res)
    except:
        return err_res(MODULE_NOT_FOUND_ERR)

@app.route("/api/update_module_code", methods = ['POST'])
def update_module_code():
    '''
    body:
        name = 'xxx' (fqn)
        code = ['line1', 'line2', ...]
    function: update the module's code
    '''
    run_method_start = None
    run_method_end = None

    try:
        module_fqn = request.form["fqn"]
        file_name = get_filepath_from_moduleFqn(module_fqn)
        module_name = get_module_name_from_fqn(module_fqn)
    except:
        return err_res(MODULE_NOT_PROVIDED_ERR)

    try:
        module_code = request.form['code']
    except:
        return err_res(CODE_NOT_PROVIDED_ERR)

    module_code = json.loads(module_code)
    module_code = [line.encode('utf-8') for line in module_code]

    # if file exists.. create copy
    if os.path.isfile(file_name):
        duplicate_file_name = file_name[:-3] + "_smv_update_code_duplicate.py" # -3 is len(".py")
        copyfile(file_name, duplicate_file_name)

        lines_of_code = None                    # counter for number of lines of code
        lines_of_code_list = [];                # list containing a string for each line of code

        # open duplicate and update its run method with code input
        with open(duplicate_file_name, 'r+') as fd:
            lines_of_code_list = fd.readlines()
            lines_of_code = len(lines_of_code_list)

            # TODO: temporarily updating class code, not run method.
            # (run_method_start, run_method_end) = get_SMV_module_run_method_start_end(lines_of_code_list)
            (run_method_start, run_method_end) = get_SMV_module_class_start_end(lines_of_code_list, module_name)

            # determine what to do if run method not provided
            if not run_method_start and not run_method_end:
                return 'no run method found in module' # TODO error message

            code_before_run_method = lines_of_code_list[:run_method_start]
            code_after_run_method = lines_of_code_list[run_method_end:]
            new_run_method_code = module_code
            updated_code = code_before_run_method + new_run_method_code + code_after_run_method

            # modify duplicate
            fd.seek(0)  # reset file stream
            fd.truncate()
            # for i in xrange(len(updated_code)):
            #    fd.write(updated_code[i])

            # will write whole code to file.. not just class or run method
            for i in xrange(len(module_code)):
                fd.write(module_code[i])

        # compile duplicate
        compile_has_errors = test_compile_for_errors(duplicate_file_name)
        print "compile_errors: " + str(compile_has_errors)

        # remove duplicate and its .pyc
        os.remove(duplicate_file_name)
        if os.path.isfile(duplicate_file_name + 'c'):
            os.remove(duplicate_file_name + 'c')

        if compile_has_errors:
            return err_res(COMPILATION_ERROR, "Module failed to compile", compile_has_errors)

        # if duplicate's compile successfull, modify the code of the original file
        with open(file_name, 'w') as fd:
            # for i in xrange(len(updated_code)):
            #     fd.write(updated_code[i])
            for i in xrange(len(module_code)):
                fd.write(module_code[i])

        return ok_res(JOB_SUCCESS)
    else:
        return err_res(MODULE_NOT_FOUND_ERR)

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
    res = SmvApp.getInstance().get_graph_json()
    return jsonify(graph=res)

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
    smvApp = SmvApp.createInstance([])

    module_file_map = {}

    # start server
    host = os.environ.get('SMV_HOST', '0.0.0.0')
    port = os.environ.get('SMV_PORT', '5000')
    project_dir = os.environ.get('PROJECT_DIR', './')
    app.run(host=host, port=int(port))
