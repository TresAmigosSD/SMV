import sys

from pyspark import SparkContext
from pyspark.sql import HiveContext
from smv import SmvApp

if __name__ == "__main__":
    # skip the first argument, which is this program
    smv = SmvApp.init(sys.argv[1:])
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
