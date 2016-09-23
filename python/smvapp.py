import sys

from pyspark import SparkContext
from pyspark.sql import HiveContext
from smv import Smv

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
        smv.run_python_module(name)

    table = app.config().exportHive()
    if (table.isDefined()):
        app.verifyConfig()
        app.exportHive(smv.run_python_module(mods[0])._jdf, table.get())
