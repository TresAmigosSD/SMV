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

from unittest import *

from pyspark import SparkContext
from pyspark.sql import HiveContext

from smv import SmvApp

import sys

# shared spark and sql context
class TestConfig(object):
    @classmethod
    def sparkContext(cls):
        if not hasattr(cls, 'sc'):
            cls.sc = SparkContext(appName="SMV Python Tests")
        return cls.sc

    @classmethod
    def sqlContext(cls):
        if not hasattr(cls, 'sqlc'):
            cls.sqlc = HiveContext(cls.sparkContext())
        return cls.sqlc

if __name__ == "__main__":
    print("Testing with Python " + sys.version)

    TestPath = "./src/test/python"
    SrcPath = "./src/main/python"

    app = SmvApp()
    app.prepend_source(SrcPath)

    loader = TestLoader()

    import sys
    argv = sys.argv[1:]
    if (len(argv) == 0):
        suite = loader.discover(TestPath)
    else:
        sys.path.append(TestPath)
        suite = loader.loadTestsFromNames(argv)

    result = TextTestRunner(verbosity=2).run(suite)
    print("result is ", result)
    exit(len(result.errors) + len(result.failures))
