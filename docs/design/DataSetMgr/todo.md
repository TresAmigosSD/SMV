Todo list for implementation of DataSetMgr
Note that all changes are being devloped in the loaduni branch

- [x] Generalize runParams to map SmvExtModule to DataFrame by FQN
- [ ] Transition from requiresDS to resolvedRequiresDS internally
  - [x] Add resolvedRequiresDS field
  - [x] Replace all references to requiresDS with resolvedRequiresDS, except where used to set resolvedRequiresDS
  - [ ] Throw SmvRuntimeException when requireDS invoked on SmvExtModule or SmvExtModulePython


- [ ] De-ignore tests and ensure they now pass
  - [ ] SmvAppDynamicResolveTest
  - [ ] ModuleCrcConsistencyTest
  - [ ] SmvLinkFollowTest


- [ ] Add test cases for
  - [ ] SmvAncillary to ensure changes to requiresDS haven't broken anything
