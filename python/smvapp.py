import sys

from pyspark import SparkContext
from pyspark.sql import SQLContext, HiveContext

# dynamically load a class by its name
# equivalent to Java's Class.forName
def for_name(name):
    components = name.split('.')
    mod = __import__(components[0])
    for comp in components[1:]:
        mod = getattr(mod, comp)
    return mod

# runs the module
def run_module(app, name):
    mod = for_name(name)
    # TODO: run dependent modules first
    mod().compute(app)

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

    smv = sc._jvm.org.tresamigos.smv.python.SmvPythonAppFactory()
    app = smv.init(java_args, sqlContext._ssql_ctx)

    print("----------------------------------------")
    print("will run the following modules:")
    mods = app.moduleNames()
    for name in mods:
        print("   " + name)
    print("----------------------------------------")

    for name in mods:
        run_module(app, name)
