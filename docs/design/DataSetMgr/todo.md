Todo list for implementation of DataSetMgr
Note that all changes are being devloped in the loaduni branch

[] Generalize runParams to map SmvExtModule to DataFrame correctly
[] Transition from requiresDS to resolvedRequiresDS internally
  [x] Add resolvedRequiresDS field
  [] Replace all references to requiresDS with resolvedRequiresDS, except where used to set resolvedRequiresDS
  [] Throw SmvRuntimeException when requireDS invoked on SmvExtModule or SmvExtModulePython

[] De-ignore tests and ensure they now pass
  [] SmvAppDynamicResolveTest
  [] ModuleCrcConsistencyTest
  [] SmvLinkFollowTest
