#!/usr/bin/env python

import os
import sys
import fnmatch
import re
from flask import Flask, request, Response
app = Flask(__name__)

def get_full_module_name(module_name, file_path):
    file_prefix = APP_HOME + '/src/main/'
    module_library = file_path.replace(file_prefix, '').split('/')[1:-1]
    module_library.append(module_name)
    full_module_name = '.'.join(module_library)
    return full_module_name

def get_all_files_with_suffix(suffix):
    '''
    This function recurseively searches for all the files with certain suffix
    under smv code path and return the absolute file names in a list.
    '''
    matches = []
    for root, dirnames, filenames in os.walk(CODE_DIR):
        for filename in fnmatch.filter(filenames, '*.%s' % suffix):
            matches.append(os.path.join(root, filename))
    return matches

def get_module_file():
    '''
    This function returns a dictionary where the key is the full module name and
    the value is the absolute path of the file containing this module.
    '''
    module_dict = {}
    # 1. search and map in Scala files
    regexp = re.compile(r'(SmvModule|SmvHiveTable|SmvCsvFile)\(')
    scala_files = get_all_files_with_suffix('scala')
    for file_path in scala_files:
        with open(file_path, 'rb') as f:
            for line in f.readlines():
                if regexp.search(line) is not None:
                    module_name = line.split()[1]
                    module_name = get_full_module_name(module_name, file_path)
                    module_dict[module_name] = file_path
    # 2. search and map in Python files
    # TODO
    return module_dict

def get_file(filename):
    try:
        return open(filename).read()
    except IOError as exc:
        return str(exc)

@app.route("/module_code", methods = ['GET'])
def return_module_code():
    '''
    Return module code
    '''
    module_name = request.args.get('name', 'NA')
    content = get_file(MODULE_FILE_DICT[module_name])
    return Response(content, mimetype="text/html")

@app.route("/module_schema", methods = ['GET'])
def return_module_schema():
    '''
    TODO: return module's schema
    '''
    module_name = request.args.get('name', 'NA')
    return None

@app.route("/module_output", methods = ['GET'])
def return_module_output():
    '''
    TODO: return module's output
    '''
    module_name = request.args.get('name', 'NA')
    return None

if __name__ == "__main__":
    APP_HOME = sys.argv[1]
    CODE_DIR = APP_HOME + '/src'
    MODULE_FILE_DICT = get_module_file()
    app.run()
