#!/usr/bin/env python

import os
import sys
import fnmatch
from flask import Flask, request
app = Flask(__name__)

def get_all_files_with_suffix(path, suffix):
    '''
    This function recurseively searches for all the files with certain suffix
    under certain path and return the absolute file names in a list.
    '''
    matches = []
    for root, dirnames, filenames in os.walk(path):
        for filename in fnmatch.filter(filenames, '*.%s' % suffix):
            matches.append(os.path.join(root, filename))
    return matches

def get_module_info():
    '''
    This function returns a dictionary where the key is the module name and the
    value is the absolute file path of this module.
    '''
    code_dir = APP_HOME + '/src'
    scala_files = get_all_files_with_suffix(code_dir, 'scala')
    python_files = get_all_files_with_suffix(code_dir, 'py')
    module_dict = {}
    #TODO: generate mudule's dict
    return module_dict

@app.route("/module_code", methods = ['GET'])
def return_module_code():
    '''
    TODO: return module's code
    '''
    module_name = request.args.get('name', 'NA')
    return module_name

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
    MODULE_DICT = get_module_info()
    app.run()
