# Python Module Reloading

## Issues Reloading Python Code

There are currently several issues (#641, #642, #646, etc.) which result from complications in managing the reloading of client Python code within a transaction. We could solve each of them individually, introducing additional nuances and complications to the process of loading modules, but it is preferable to come to a more general solution both for simplicity and to prevent similar future problems.

## Desired Behavior

Within a transaction, we want every module needed within the transaction to be loaded from the most recent source _once_ (#646 has illustrates a problem that can arise from loading a module twice in a transaction).

## Initial Approach

We initially tried to approximate this behavior by reloading a module using the builtin `reload` function every time we load one of its member `SmvDataSets` (putting it briefly). Complications have arisen when we want to assure that the module is loaded only once, when we want to make sure that inheritance dependencies are also reloaded, etc.

Using the built-in `reload` also causes some potentially unexpected behaviour for users as reloaded modules are different objects (would fail `instanceof` checks) and sub-modules are not reloaded recursively.

## Current Approach

 Essentially, during each transaction client code should be loaded as though it was run in a fresh interpreter. We accomplish this by removing all client modules from `sys.modules` at the beginning of each transaction (a similar strategy is outlined [here](https://www.safaribooksonline.com/library/view/python-cookbook/0596001673/ch14s02.html)). We may need to suppress creation of `.pyc` files (which would also solve #612) if Python falls back on these instead of recompiling. We also will need to investigate if this has unintended consequences for the `linecache` - we may need to invalidate the entire `linecache` at the beginning of the transaction instead of on a case by case basis. However, the objective of this approach is to force the interpreter to forget about client modules it imported before the current transaction, compile them the first time they are imported during the current transaction, and import them without compilation every subsequent time they are imported within the current transaction.

### Sys Module Popping strategies
We investigated a few different strategies for popping modules from `sys.modules` for the purposes described above:

1. **Name-based:** We assemble a set of module names that comprise the application code based off of the applications stages and libraries as enumerated in the application configuration props. Then, we iterate through `sys.modules` and pop each module that starts with a stage/library name followed by a `.` character to de-cache the modules at the python level. For example, for an application with a stage named `foo` and module in `sys.modules` named `foo.bar`, `foo.bar.baz` and `foozeball`, we will pop `foo.bar` and `foo.bar.baz` but not `foozeball`. This is the solution we use currently.
2. **Path-based:** We investigated a solution where the `__file__` path of each module was used to try to determine if the module was part of application code (ie: the module is from `<app-directory>/src/main/python` or `<app-directory>/library`). This approach won't work because at the start of each transaction, we decache the modules from the last transaction that may collide. Since app directory is _dynamic_ this approach doesn't gurantee that potentially colliding modules will be decached. For example, the same project clones in two different locations and run one after the other in the same session will break this approach.
3. **Inpsection-based:** We investigated a solution similar to the first solution for stages, but aimed at removing the necessity to enumerate libraries in the application configuration props by inspecting each application module (ie `foo.bar`) for its members and reloading them if they were a class or module. This approach added significant performance drag and greatly increased the complexity of the reloading code, so we rejected it in favor of approach 1 of this list.

> **Update: Behavior Changes -**
> Users should not expect significant changes to the behavior of SMV outside of fixes for the described bugs. However, since the implementation we have noticed changes to certain internal assumptions about reloading modules.

### Module identity and reference counting

 When reloading a module using Python's built-in `reload`, no new module is created - the resulting module has different attributes, but the same identity as the original. However, after deleting a module from `sys.modules` and importing it again, the resulting module is a new object with a new identity. This is makes sense - it is the behavior we originally expected from reload. However, we did not anticipate that deleting a module from `sys.modules` may reduce the module's reference count to zero, causing that module to get cleaned up. We can ensure that this will never affect users by 1. never giving users direct access to modules and 2. never reusing modules from a transaction after a second transaction has been started; however, this led to a strange bug in our Python unit test suite. The scenario was that `testDataFrameHelper` contained a module `T` that was used in several tests, including `test_smvExportCsv`, where it was run using `SmvApp.runModule`. Immediately after `runModule`, all of the module's global names resolved to `None`. This was difficult to replicate outside of the test suite. We eventually discovered that maintaining a reference to the old `testDataFrameHelper` module prevented this problem, and from there worked out that when we removed the `sys.modules's` reference to `testDataFrameHelper` we were removing the last one, causing `testDataFrameHelper` to get cleaned up in the process. It appears that when a module is cleaned up, at least some of the mappings in its globals dict are overwritten with `None`. Functions defined inside the module are still, however, using that same globals dict to look up names dynamically, and as a result getting `None` for names which a shortly ago were defined.
