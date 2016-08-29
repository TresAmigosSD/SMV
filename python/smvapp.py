import sys

from pyspark import SparkContext
from pyspark.sql import SQLContext, HiveContext

if __name__ == "__main__":

    # initialize SparkContext from Python to get the python-java gateway
    sc = SparkContext(appName="smvapp.py")
    sqlContext = HiveContext(SQLContext(sc))

    # convert python arglist to java String array
    # skip the first argument, which is this program
    arglist = sys.argv[1:]
    java_args = sc._gateway.new_array(sc._jvm.String, len(arglist))
    for i in range(0, len(java_args)):
        java_args[i] = arglist[i]

    proxy = sc._jvm.org.tresamigos.smv.python.SmvPythonProxy()
    proxy.runMain(java_args, sqlContext._ssql_ctx)
