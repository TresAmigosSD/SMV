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
from pyspark.sql import SparkSession

from smv import SmvApp as app

import sys

# shared spark and sql context
class TestConfig(object):
    @classmethod
    def sparkSession(cls):
        if not hasattr(cls, "spark"):
            cls.spark = SparkSession.builder.appName("SMV Python Tests").enableHiveSupport().getOrCreate()
        return cls.spark

    @classmethod
    def sparkContext(cls):
        return cls.sparkSession().sparkContext

if __name__ == "__main__":
    print("Testing with Python " + sys.version)

    TestPath = "./src/test/python"
    SrcPath = "./src/main/python"

    app.add_source(SrcPath)

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
