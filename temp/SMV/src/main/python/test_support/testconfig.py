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

from pyspark import SparkContext, SparkConf
from pyspark.sql import HiveContext, SparkSession
from pyspark.java_gateway import launch_gateway

from smv.smvapp import SmvApp

class TestConfig(object):
    smvApp = None

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
            import tempfile
            import getpass
            hivedir = "file://{0}/{1}/smv_hive_test".format(tempfile.gettempdir(), getpass.getuser())
            sConf = SparkConf(False, _jvm=jvm).set("spark.sql.test", "")\
                                              .set("spark.sql.hive.metastore.barrierPrefixes",
                                                   "org.apache.spark.sql.hive.execution.PairSerDe")\
                                              .set("spark.sql.warehouse.dir", hivedir)\
                                              .set("spark.ui.enabled", "false")
            sc = SparkContext(master="local[1]", appName="SMV Python Test", conf=sConf, gateway=jgw).getOrCreate()
            jss = sc._jvm.org.apache.spark.sql.hive.test.SmvTestHive.createContext(sc._jsc.sc())
            cls.spark = SparkSession(sc, jss.sparkSession())
        return cls.spark

    @classmethod
    def setSmvApp(cls, app):
        """Set the canonical SmvApp

            Spark context and sqlContext will be retrieved from this SmvApp.
            This SmvApp will also be restored as the singleton after tests are
            run.
        """
        cls.smvApp = app
        cls.sqlc = app.sqlContext
        cls.sc = app.sc

    @classmethod
    def originalSmvApp(cls):
        return cls.smvApp

    # shared SparkContext
    @classmethod
    def sparkContext(cls):
        return cls.sparkSession().sparkContext

    # shared HiveContext
    @classmethod
    def sqlContext(cls):
        if not hasattr(cls, 'sqlc'):
            cls.sqlc = HiveContext(cls.sparkContext())
        return cls.sqlc

    # smv args specified via command line
    @classmethod
    def smv_args(cls):
        if not hasattr(cls, '_smv_args'):
            cls.parse_args()
        return cls._smv_args

    # test names specified via command line
    @classmethod
    def test_names(cls):
        if not hasattr(cls, '_test_names'):
            cls.parse_args()
        return cls._test_names

    @classmethod
    def test_path(cls):
        if not hasattr(cls, '_test_path'):
            cls.parse_args()
        return cls._test_path

    # Parse argv to split up the the smv args and the test names
    @classmethod
    def parse_args(cls):
        args = sys.argv[1:]
        test_names = []
        test_path = "./src/test/python"
        smv_args = []
        while(len(args) > 0):
            next_arg = args.pop(0)
            if(next_arg == "-t"):
                test_names.append( args.pop(0) )
            elif(next_arg == "-d"):
                test_path = args.pop(0)
            else:
                smv_args.append(next_arg)

        cls._test_names = test_names
        cls._test_path = test_path
        cls._smv_args = smv_args
