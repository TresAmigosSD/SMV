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
import glob
from flask import Flask, request, jsonify
from smv import SmvContext
import compileall

app = Flask(__name__)

# ---------- Helper Functions ---------- #

def compile_python_files():
    r = compileall.compile_dir('src/main/python', quiet=1)
    if not r:
        exit(-1)

def print_module_names(mods):
    print("----------------------------------------")
    print("will run the following modules:")
    for name in mods:
        print("   " + name)
    print("----------------------------------------")

def get_output_dir():
    output_dir = SMV_CTX.outputDir()
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

# ---------- API Definition ---------- #

@app.route("/run_modules", methods = ['GET'])
def run_modules():
    '''
    Take FQNs as parameter and run the modules
    '''
    module_name = request.args.get('name', 'NA')
    # create SMV app instance
    arglist = ['-m'] + module_name.strip().split()
    smv_app = SMV_CTX.create_smv_app(arglist)
    # run modules
    modules = smv_app.moduleNames(SMV_CTX.repo)
    print_module_names(modules)
    publish = smv_app.publishVersion().isDefined()
    for name in modules:
        if publish:
            SMV_CTX.publishModule(name)
        else:
            SMV_CTX.runModule(name)
    return ''

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

if __name__ == "__main__":
    compile_python_files()

    # initialize a SMV context
    config = {
        'name': 'SmvServer',
        'log_level': 'ERROR',
    }
    SMV_CTX = SmvContext(config)

    # start server
    app.run()
