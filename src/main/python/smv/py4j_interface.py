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
import traceback

def create_py4j_interface_method(interface_method_name, impl_method_name):
    def interface_method(obj, *args):
        impl_method = getattr(obj, impl_method_name)
        try:
            result = impl_method(*args)
            response = SmvPy4JValidResponse(result)
        except Exception as e:
            error = traceback.format_exc()
            response = SmvPy4JErrorResponse(error)
        return response
    interface_method.__name__ = interface_method_name
    return interface_method

class SmvPy4JResponse(object):
    def __init__(self, _successful, _result, _error):
        self._successful = _successful
        self._result = _result
        self._error = _error

    def successful(self):
        return self._successful

    def result(self):
        return self._result

    def error(self):
        return self._error

    class Java:
        implements = ["org.tresamigos.smv.IPythonResponsePy4J"]

class SmvPy4JErrorResponse(SmvPy4JResponse):
    def __init__(self, error):
        super(SmvPy4JErrorResponse, self).__init__(False, None, error)

class SmvPy4JValidResponse(SmvPy4JResponse):
    def __init__(self, result):
        super(SmvPy4JValidResponse, self).__init__(True, result, None)
