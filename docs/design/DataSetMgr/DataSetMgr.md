# DataSetMgr service in SmvApp

When running an `SmvModule`s dynamically (and when running `SmvModule`s for the first time) by name in `SmvApp`, we first use one approach to look up Scala modules, then another to search each registered `SmvDataSetRepository` for external modules. For simplification, it is desirable to create a unified repository interface used to search for modules in any language. This interface will be responsible to find constituent `SmvDataSet`s by name and load them from the most recent source.

It is also desirable that `SmvApp` itself be agnostic of how `SmvDataSet`s are discovered in the first place. This motivates the creation of a singular service to manage and coordinate between the repositories and, more generally, to unify `SmvDataSet` lookup to a single interface agnostic of the underlying lookup mechanisms. This interface will field all app-level requests for `SmvDataSet`s by name.


# Deprecation of df behavior

`smv-shell` and `smv-pyshell` currently two convenient methods for resolving modules to `DataFrames`: `df` and `ddf`. `df` takes an `SmvDataSet` and returns its resulting `DataFrame`. `ddf` takes the URN of an `SmvDataSet` as a string, reloads the `SmvDataSet` class, then returns the resulting `DataFrame`. The design outlined in this document presumes that `df` will take on the behavior of `ddf` (and `ddf` will be phased out); i.e. that `df` will aways reload the `SmvDataSet` before resolving it to an `DataFrame`. This design could hypothetically support the non-'dynamic' behavior of `df`, but this would complicate things and is likely unnecessary because the distinction only exists in the shell, where users will want to run the most recently written version of a module.


# DataSetMgr interface

At the level of `SmvApp`, all requests to load `SmvDataSet`s will be delegated to `DataSetMgr`. `DataSetMgr` will provide a method `loadDataSetWithDep(fqn: string)` to load an `SmvDataSet` and its dependencies by name. This method will always load the `SmvDataSet` as defined in the most recent source.


# Repositories

Presently, the Scala `SmvApp` queries for Python modules through the `SmvDataSetRepository` Java interface. Rather than create a Scala module repository which implements this interface (which would be circular), we will create generic dataset repository class called `DataSetRepo` from which a Scala repository called `DataSetRepoScala` and a Python repository called `DataSetRepoPython` will inherit. The `DataSetRepoPython` will wrap the Java interface, which will be renamed `IDataSetRepoPy4J`. These classes each must implement a method `loadDataSet(fqn: String)`. The `DataSetMgr` will resolve an `SmvDataSet` by name by asking its containing repo to load it, then recursively doing the same for each of the `SmvDataSet`s specified in the resolved `SmvDataSet`'s dependency list. To prevent loading the same `SmvDataSet` twice, `DataSetMgr` will track the `SmvDataSet`s loaded during the current transaction in a `Map[String, SmvDataSet]` called `DataSetByName`.



# DataFrame Caching

SMV and Spark perform various types of caching to minimize calculations and also for resiliency in case of failure. One cache kept by SMV is of the DataFrame (or RDD) that each `SmvDataSet` resolves to. Without this cache, an `SmvDataSet`'s DAG would be recaulcuated every time is resolved to an RDD.

Currently, the DataFrame cache for each `SmvDataSet` is internal to the `SmvDataSet`. This means that we lose its cache _each time it's reloaded_. This is acceptable under the assumption that the user reloads the `SmvDataSet` (by running the module dynamically) only when they have made changes and want to see the result. However, if the `SmvDataSet` will be reloaded every time it is resolved by name we will need to move the cache somewhere that is persistent when the `SmvDataSet` reloads so that the DataFrame can be reused if no changes have been made.


# Resolution of Dependencies

Dependencies for a `SmvModule` are specified by the user through the module's `requireDS()`. This method is then called internally e.g. in the `SmvModule`'s `doRun()`. If the user lists an `SmvExtModule` as a dependency, e.g. `requireDS = List(SmvExtModule('a.b.c'))`, a new ``SmvExtModule`` is instantiated _every_ time `requireDS()` is invoked - note that the user is in fact supplying a function, not a static value. Also, when an `SmvExtModule` is instantiated, the module will immediately try to get the external module from `DataSetMgr`. This presents a problem (probably multiple problems) for our design.

Consider the following dependency scenario
```
    x
  /   \
'y'    z
       |
      'y'
```
where `x` and `z` are both Scala `SmvModule`s that depend through an `SmvExtModule` on a Python `SmvPyModule` `y`, which may itself have an extensive dependency tree.

When `x` is resolved, it will instantiate both `z` _and_ an instance of `SmvExtModule` which will try to resolve `'y'` to an `ISmvModulePy4J`, implicitly instantiating the Python module `y`. When `z` instantiates it _also_ will instantiate an `SmvExtModule`, to the same effect. Among the problems created here is that there is now a duplicate instance of `y` in dependencies of `x`.

The solution is a bit complicated but manageable,

1. We differentiate between the `SmvExtModule` declared by the user when listing dependencies and a new external module class, which we will call `SmvExtModulePython`, used on the back end. `SmvExtModule` is a placeholder that _does not trigger the instantiation of the external module_.  `SmvExtModulePython`, created only by `DataSetRepoPython`, is the `SmvModule` wrapper for `ISmvModulePy4J`.

2. We differentiate between `requireDS()`, and a fixed list of dependency names we will call `reqUrns`, and a fixed list of resolved `SmvDataSet` dependencies we will call `reqRes`. `requireDS()` will be called internally only once: when the module is instantiated. Its result will be mapped to URNs to define `reqUrns`. A method of `SmvModule` called `resolveReq(resolver: String => `SmvDataSet`)` will set `reqRes` by mapping each URN to an `SmvDataSet` using the `resolver` function parameter. `DataSetMgr` will in fact provide the `resolver`, but `SmvModule` is agnostic of this.

The previous scenario is reworked under this solution in the sequence diagram.
