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
from flask import Flask, request
from smv import SmvContext
import compileall

app = Flask(__name__)

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
