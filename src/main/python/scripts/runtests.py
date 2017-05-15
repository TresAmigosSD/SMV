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
from unittest import *
from test_support.testconfig import TestConfig

if __name__ == "__main__":
    print("Testing with Python " + sys.version)

    TestPath = "./src/test/python"
    SrcPath = "./src/main/python"

    loader = TestLoader()

    if (len(TestConfig.test_names()) == 0):
        suite = loader.discover(TestPath)
    else:
        sys.path.append(TestPath)
        suite = loader.loadTestsFromNames(TestConfig.test_names())

    result = TextTestRunner(verbosity=2).run(suite)
    print("result is ", result)
    exit(len(result.errors) + len(result.failures))
