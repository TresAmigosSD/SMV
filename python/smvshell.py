from smv import SmvApp
from pyspark.sql import DataFrame

jvmShellCmd = SmvApp._jvm.org.tresamigos.smv.shell.ShellCmd

pdf = lambda fqn: SmvApp.run_python_module(fqn)
ddf = lambda fqn: DataFrame(jvmShellCmd.ddf(fqn), SmvApp.sqlContext)
openHive = lambda tableName: DataFrame(jvmShellCmd.openHive(tableName), SmvApp.sqlContext)
openCsv = lambda path: DataFrame(jvmShellCmd.openCsv(path), SmvApp.sqlContext)

def lsStage():
    print(jvmShellCmd.lsStage())

def ls(stageName = None):
    if(stageName is None):
        print(jvmShellCmd.ls())
    else:
        print(jvmShellCmd.ls(stageName))

def lsDead(stageName = None):
    if(stageName is None):
        print(jvmShellCmd.lsDead())
    else:
        print(jvmShellCmd.lsDead(stageName))

def lsLeaf(stageName = None):
    if(stageName is None):
        print(jvmShellCmd.lsLeaf())
    else:
        print(jvmShellCmd.lsLeaf(stageName))

def graph(stageName = None):
    if(stageName is None):
        print(jvmShellCmd._graph())
    else:
        print(jvmShellCmd._graph(stageName))

def now():
    print(jvmShellCmd.now())
