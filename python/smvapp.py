import sys

from pyspark import SparkContext
from pyspark.sql import SQLContext, HiveContext
from smv import Smv

# dynamically load a class by its name
# equivalent to Java's Class.forName
def for_name(name):
    components = name.split('.')
    mod = __import__(components[0])
    for comp in components[1:]:
        mod = getattr(mod, comp)
    return mod

# runs the module
def run_module(smv, name):
    mod = for_name(name)
    # TODO: run dependent modules first
    mod().compute(smv)

if __name__ == "__main__":

    # initialize SparkContext from Python to get the python-java gateway
    sc = SparkContext(appName="smvapp.py")
    sqlContext = HiveContext(sc)

    # skip the first argument, which is this program
    smv = Smv(sys.argv[1:], sqlContext)
    app = smv.app

    print("----------------------------------------")
    print("will run the following modules:")
    mods = app.moduleNames()
    for name in mods:
        print("   " + name)
    print("----------------------------------------")

    for name in mods:
        run_module(smv, name)
