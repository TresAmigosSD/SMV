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

from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession

from pyspark.java_gateway import launch_gateway

import sys

# shared spark and sql context
class TestConfig(object):
    @classmethod
    def sparkSession(cls):
        if not hasattr(cls, "spark"):
            # We can't use the SparkSession Builder here, since we need to call
            # Scala side's SmvTestHive.createContext to create the HiveTestContext's
            # SparkSession.
            # So we need to
            #   * Create a java_gateway
            #   * Create a SparkConf using the jgw (since without it SparkContext will ignore the given conf)
            #   * Create python SparkContext using the SparkConf (so we can specify the warehouse.dir)
            #   * Create Scala side HiveTestContext SparkSession
            #   * Create python SparkSession
            jgw = launch_gateway(None)
            jvm = jgw.jvm
            sConf = SparkConf(False, _jvm=jvm).set("spark.sql.test", "")\
                  .set("spark.sql.hive.metastore.barrierPrefixes",
                    "org.apache.spark.sql.hive.execution.PairSerDe")\
                  .set("spark.sql.warehouse.dir", "file:///tmp/smv_hive_test")\
                  .set("spark.ui.enabled", "false")
            sc = SparkContext(master="local[1]", appName="SMV Python Test", conf=sConf, gateway=jgw).getOrCreate()
            jss = sc._jvm.org.apache.spark.sql.hive.test.SmvTestHive.createContext(sc._jsc.sc())
            cls.spark = SparkSession(sc, jss.sparkSession())
        return cls.spark

    @classmethod
    def sparkContext(cls):
        return cls.sparkSession().sparkContext


    @classmethod
    def tearDown(cls):
        cls.sparkContext()._jvm.org.apache.spark.sql.hive.test.SmvTestHive.destroyContext()
        cls.spark.stop()
        del cls.spark

if __name__ == "__main__":
    print("Testing with Python " + sys.version)

    TestPath = "./src/test/python"
    SrcPath = "./src/main/python"

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
