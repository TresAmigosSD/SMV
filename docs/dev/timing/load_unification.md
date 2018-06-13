# Benchmarking the unified loading scheme

To benchmark module loading after the unification, the component tasks of `SmvApp.modulesToRun` were timed for the scenario that the user invokes `smv-run --run-app` on an application of 1000 modules with a linear dependency. The tasks taking the most time were in turn decomposed for further timing. `modulesToRun` was decomposed as follows:

```
modulesToRun
  |-  find directModsL (0.00s)
  |-  find stageMods (.045s)
  |-  find appMods (7.26s)
        |-  discover urns (2.18s)
        |-  load (4.73s)
              |-  (other)
              |-  findDataSetInRepo (4.43s)
                    |- (other)
                    |- DataSetRepoPython.loadDataSet (3.183s)

```
We noted that that every time a module a Python is reloaded, all of its dependencies are also reloaded, which would imply that loading a Python module is O(d<sup>2</sup>) where d is the depth of the dependency tree. However, a project with 1000 modules and a depth of 1 saw only a 1 second improvement in load time, so there are other factors at play.
