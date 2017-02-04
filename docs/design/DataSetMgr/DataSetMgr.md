# DataSetMgr service in SmvApp

When running an `SmvModule`s dynamically (and when running `SmvModule`s for the first time) by name in `SmvApp`, we first use one approach to look up Scala modules, then another to search each registered `SmvDataSetRepository` for external modules. For simplification, it is desirable to create a unified repository interface used to search for modules in any language. This interface will be responsible to find constituent `SmvDataSet`s by name and load them from the most recent source.

It is also desirable that `SmvApp` itself be agnostic of how `SmvDataSet`s are discovered in the first place. This motivates the creation of a singular service to manage and coordinate between the repositories and, more generally, to unify `SmvDataSet` lookup to a single interface agnostic of the underlying lookup mechanisms. This interface will field all app-level requests for `SmvDataSet`s by name.


# Deprecation of df behavior

`smv-shell` and `smv-pyshell` currently two convenient methods for resolving modules to `DataFrames`: `df` and `ddf`. `df` takes an `SmvDataSet` and returns its resulting `DataFrame`. `ddf` takes the URN of an `SmvDataSet` as a string, reloads the `SmvDataSet` class, then returns the resulting `DataFrame`. The design outlined in this document presumes that `df` will take on the behavior of `ddf` (and `ddf` will be phased out); i.e. that `df` will aways reload the `SmvDataSet` before resolving it to an `DataFrame`. This design could hypothetically support the non-'dynamic' behavior of `df`, but this would complicate things and is likely unnecessary because the distinction only exists in the shell, where users will want to run the most recently written version of a module.


# Module loading

We define the term 'load' in this document as follows: to load a module is to create a class object for that object _from the most recent source_ and subsequently instantiate a single object of that class.


# DataSetMgr interface

At the level of `SmvApp`, all requests to load `SmvDataSet`s will be delegated to `DataSetMgr`. `DataSetMgr` will provide a method `loadDataSetWithDep(fqn: String)` to load an `SmvDataSet` and its dependencies by name. This method will always load the `SmvDataSet` as defined in the most recent source.


# Repositories

Presently, the Scala `SmvApp` queries for Python modules through the `SmvDataSetRepository` Java interface. Rather than create a Scala module repository which implements this interface (which would be circular), we will create generic dataset repository class called `DataSetRepo` from which a Scala repository called `DataSetRepoScala` and a Python repository called `DataSetRepoPython` will inherit.  `DataSetRepoPython` will wrap the Java interface, which will be renamed `IDataSetRepoPy4J`. `DataSetRepoPython` will also return an `SmvExtModulePython` - more on this later. These classes each must implement a method `loadDataSet(fqn: String)`. The `DataSetMgr` will resolve an `SmvDataSet` by name by asking its containing repo to load it, then recursively doing the same for each of the `SmvDataSet`s specified in the resolved `SmvDataSet`'s dependency list. To prevent loading the same `SmvDataSet` twice, a transaction state object will track the `SmvDataSet`s already loaded since the app level invocation of `loadDataSetWithDep(fqn: String)`


# DataFrame Caching

SMV and Spark perform various types of caching to minimize calculations and also for resiliency in case of failure. One cache kept by SMV is of the DataFrame (or RDD) that each `SmvDataSet` resolves to. Without this cache, an `SmvDataSet`'s DAG would be recalculated every time is resolved to an RDD.

Currently, the DataFrame cache for each `SmvDataSet` is internal to the `SmvDataSet`. This means that we lose its cache _each time it's reloaded_. This is acceptable under the assumption that the user reloads the `SmvDataSet` (by running the module dynamically) only when they have made changes and want to see the result. However, if the `SmvDataSet` will be reloaded every time it is resolved by name we will need to move the cache somewhere that is persistent when the `SmvDataSet` reloads so that the DataFrame can be reused if no changes have been made.


# External Dependencies and requireDS()

Dependencies for a `SmvModule` are specified by the user through the module's `requireDS()`. This method is then called internally e.g. in the `SmvModule`'s `doRun()`. If the user lists an `SmvExtModule` as a dependency, e.g. `requireDS = List(SmvExtModule('a.b.c'))`, a new `SmvExtModule` is instantiated _every time_ `requireDS()` is invoked - note that the user is in fact supplying a function, not a static value. This presents problems for our design:

### Problem 1:

Consider a simple dependency scenario
```
 x (s)
 |
 y (p)
```
where `x` and `z` are both Scala modules and `y` is a Python module. For simplicity's sake, assume none of them is ephemeral. Suppose the user runs `x` through any entrypoint.

First, `SmvApp` asks `DataSetMgr` to load `'x'`. `DataSetMgr` delegates to `DataSetRepoScala` to load `x`, then asks `x` for its dependencies through `requireDS()`, which returns a single `SmvExtModule` of `y`. Then, `DataSetMgr` delegates to `DataSetRepoPython` to load the `ISmvModulePy4J` for `y`, presumably wraps it in an `SmvExtModule` (the current wrapper for external modules) we will call `y1`, and returns `y1` to `DataSetMgr`.

Fast-forwarding a bit, `DataSetMgr` returns `x` to `SmvApp`, which runs `x` using `x.rdd()`. When `x` tries to compute its `DataFrame`, it will first try to read its cache on disk, invoking its `hashOfHash()` in the process. `hashOfHash()` recursively calls the `hashOfHash()` of each of `x`'s dependencies as discovered through `requireDS()`, including a _new_ `SmvExtModule` `y2` of `y`. Invoking `y2`'s `hashOfHash()` causes `y2` to resolve its `ISmvModulePy4J` by asking `DataSetMgr` to load `'y'`.

Modules should be singletons, but `y` has now loaded twice. This will occur again when `x` resolves the RDDs of its dependencies. Also, each time this occurs, the entire dependency tree of `y` (which may be quite large) is reloaded.

Solution: For backwards compatibility's sake, we can't change how users specify dependencies. However, we can change how internals of SMV access a module's dependencies. We will differentiate between `requireDS()`, which will remain a function defined by the user to specify a module's dependencies, and `requireDSRes`, the module's (fixed) canonical list of dependencies. `requireDSRes` would be set by a single call to `requireDS()`. Thus we will neither instantiate new `SmvExtModule`s nor reload the external module when inspecting a module's dependencies.


### Problem 2

Consider the dependency scenario
```
   x(s)
  /  \
 |   z(s)
 |   /
y(p)
```
where Scala module `x` depends on Scala module `z` and both depend on Python module `y`.

Note that when we call `x` and `z`'s respective `requiresDS()`, each will still instantiate a distinct new `SmvExtModule` of `y`. This is the problematic for the same reasons as before.

Solution: Separate the declarative functionality of `SmvExtModule` needed by the user and the external module interface functionality needed internally. `SmvExtModule` will continue to be a declarative sugar for the user, but will no longer resolve the `ISmvModulePy4J`. Instead, a new class `SmvExtModulePython` will take over this responsibility. `SmvExtModulePython` will be created only  `DataSetRepoPython`. Thus, although 2 `SmvExtModule`s of `y` are created when while loading `x`, neither of them forces a load of the Python module. In fact, `DataSetMgr` will load a `SmvExtModulePython` of `y` just once (using the same mechanism that prevents duplication of Scala modules) and this will be inserted in `x`'s `requireDSRes`. When `DataSetMgr` returns `x` to the app, its `requireDSRes` will include not an `SmvExtModule` but an `SmvExtModulePython`.


# Module Resolution

### Problem 1
`DataSetMgr` must facilitate `SmvModule` in loading its Python dependencies and resolving them as `SmvExtModulePython`s, but it is desirable that `SmvModule` know nothing about the `DataSetRepo`s or the `DataSetMgr`.

### Problem 2
Consider the dependency scenario
```
   x(s)
  /   \
y(p)  z(s)
```
where `x` and `z` are both Scala modules and `y` is a Python module.

When `DataSetMgr` loads `x`, it must ensure that the classes of `x`'s dependencies `y` and `z` are also loaded. Because of the way the Java `Classloader`s work, when the `Classloader` loads the class of `x` it will also load the class of `x`'s Scala (but not Python) dependencies, and if `DataSetMgr` invokes `x.requireDS()` it will create an instance of `z` and an instance of `SmvExtModule`; however, it will not create an instance of `y` on the Python side. Thus, we must ensure that `y` is loaded now, but `z` is not loaded a second time.

These 2 problems motivate the distinction of a new responsibility which we will term resolution of `SmvDataSet`s. Within a single `DataSetMgr.load`, to resolve an `SmvDataSet` is to

* Load its canonical class and instance if they have not already been loaded (in this transaction)
* Resolve its dependencies
* Return the canonical instance

By canonical class for an `SmvDataSet` we indicate the `SmvDataSet` class which is actually runnable. The canonical class of a subclass `C` of `SmvDataSet` is `C` itself _unless_ `C` is an `SmvExtModule`, in which case it is the `SmvExtModulePython` that corresponds to the same `SmvPyModule`. By canonical instance we indicate the 'singleton' instance of the class.

`SmvDataSet`s are already well-suited to walk their own dependency trees recursively, so they can resolve themselves if provided the logic for how and when to load dependencies. Empowering `SmvDataSet`s to resolve their own dependencies while being agnostic of the mechanism to do so will also solve problem 1.

As for the logic for how and when to load dependencies, we will create a class `DataSetResolver` to which `DataSetMgr` delegates this responsibility. Particularly, each `DataSetMgr.load` will incur the creation of a new `DataSetResolver` which will track the state for that load transaction, ensuring that only one class loader is created and datasets are only loaded once.
