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

import sys


# shared spark and sql context
class TestConfig(object):
    @classmethod
    def smv_args(cls):
        if not hasattr(cls, '_smv_args'):
            cls.parse_args()
        return cls._smv_args

    @classmethod
    def test_names(cls):
        if not hasattr(cls, '_test_names'):
            cls.parse_args()
        return cls._test_names

    # Parse argv to get split up the the smv_args and the test names
    @classmethod
    def parse_args(cls):
        args = sys.argv[1:]
        test_names = []
        smv_args = []
        while(len(args) > 0):
            next_arg = args.pop(0)
            if(next_arg == "-t"):
                test_names.append( args.pop(0) )
            else:
                smv_args.append(next_arg)

        cls._test_names = test_names
        cls._smv_args = smv_args

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

    loader = TestLoader()

    if (len(TestConfig.test_names()) == 0):
        suite = loader.discover(TestPath)
    else:
        sys.path.append(TestPath)
        suite = loader.loadTestsFromNames(TestConfig.test_names())

    result = TextTestRunner(verbosity=2).run(suite)
    print("result is ", result)
    exit(len(result.errors) + len(result.failures))
