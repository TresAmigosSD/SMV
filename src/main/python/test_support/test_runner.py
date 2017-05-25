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
from unittest import *
from inspect import ismodule

from smv.utils import for_name

class SmvTestRunner(object):
    def __init__(self, test_path):
        self.test_path = test_path

    def reload_test_mods(self, test_names):
        for name in test_names:
            test_obj = for_name(name)
            if ismodule(test_obj):
                test_mod = test_obj
            else:
                test_mod = sys.modules[obj.__module__]
            reload(test_mod)


    def run(self, test_names):
        loader = TestLoader()

        if len(test_names) == 0:
            suite = loader.discover(self.test_path)
        else:
            sys.path.append(self.test_path)
            self.reload_test_mods(test_names)
            suite = loader.loadTestsFromNames(test_names)
            sys.path.remove(self.test_path)

        result = TextTestRunner(verbosity=2).run(suite)
        print("result is ", result)
        return len(result.errors) + len(result.failures)
