Todo list for implementation of DataSetMgr
Note that all changes are being devloped in the loaduni branch

- [ ] De-ignore tests and ensure they now pass
  - [x] SmvAppDynamicResolveTest (test will be deleted as no longer relevant)
  - [ ] ModuleCrcConsistencyTest *
  - [ ] SmvLinkFollowTest *
  - [ ] SmvGraphUtilTest
  - [x] SmvExtModuleLinkTest (test was deleted as no longer relevant)

    \* = Test should pass once CRC of modules stabilizes

- [ ] Add test cases for
  - [ ] SmvAncillary to ensure changes to requiresDS haven't broken anything
  - [ ] Make sure that modules are only instantiated once per transaction
