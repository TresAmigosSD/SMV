# DataSetMgr service in SmvApp

When running `SmvModule`s dynamically (and when running `SmvModule`s for the first time) by name in `SmvApp`, we first use one approach to look up Scala modules, then another to search each registered `SmvDataSetRepository` for external modules. For simplification, it is desirable to create a unified repository interface used to search for modules in any language. This interface will be responsible to find constituent `SmvDataSet`s by name and load them from the most recent source.

It is also desirable that `SmvApp` itself be agnostic of how `SmvDataSet`s are discovered in the first place. This motivates the creation of a singular service to manage and coordinate between the repositories and, more generally, to unify `SmvDataSet` lookup to a single interface agnostic of the underlying lookup mechanisms. This interface will field all app-level requests for `SmvDataSet`s by name.


# Deprecation of df behavior

`smv-shell` and `smv-pyshell` currently supply two convenient methods for resolving modules to `DataFrames`: `df` and `ddf`. `df` takes an `SmvDataSet` and returns its resulting `DataFrame`. `ddf` takes the URN of an `SmvDataSet` as a string, reloads the `SmvDataSet` class, then returns the resulting `DataFrame`. The design outlined in this document presumes that `df` will take on the behavior of `ddf` (and `ddf` will be phased out); i.e. that `df` will aways reload the `SmvDataSet` before resolving it to an `DataFrame`. This design could hypothetically support the non-'dynamic' behavior of `df`, but this would complicate things and is likely unnecessary because the distinction only exists in the shell, where users will want to run the most recently written version of a module.


# Module loading

We define the term 'load' in this document as follows: to load a module is to create a class for that module _from the most recent source_ and subsequently instantiate a single object of that class.

# URN

We use Universal Resource Names (URN) to uniquely identify `SmvDataSets`. A URN in SMV takes the form "type:fqn", where fqn is the Fully Qualified Name (FQN) of an `SmvDataSet` that is *not a SmvModuleLink*. type can be "link", indicating an `SmvModuleLink` to the `SmvDataSet` with that fqn (specifically, the one that is not an SmvModuleLink).

Up to this point we have represented URNs and FQNs both as strings. Moving forward, URN will be represented by the abstract class `URN` implemented by `ModURN` and `LinkURN`. This enforces the expectation that an argument is a URN and not an FQN and also enables pattern matching.


# DataSetMgr interface

At the level of `SmvApp`, all requests to load `SmvDataSet`s will be delegated to `DataSetMgr`. `DataSetMgr` will provide a method `load(fqn: *String)` to load an `SmvDataSet` and its dependencies by name. This method will always load the `SmvDataSet` as defined in the most recent source. `DataSetMgr` will also provide methods for discovery of `SmvDataSets` (e.g. `allOutputModules`) and a method `inferDS(partialName: *String)` for inference of which `SmvDataSet` is indicated by a partial name. This unifies tools for finding `SmvDataSets` which are currently dispersed throughout SMV.


# Repositories

Presently, the Scala `SmvApp` queries for Python modules through the `SmvDataSetRepository` Java interface. Rather than create a Scala module repository which implements this interface, we will create generic dataset repository class called `DataSetRepo` from which a Scala repository called `DataSetRepoScala` and a Python repository called `DataSetRepoPython` will inherit.  `DataSetRepoPython` will wrap the Java interface, which will be renamed `IDataSetRepoPy4J`. `DataSetRepoPython` will also return an `SmvExtModulePython` - more on this later. These classes each must implement a method `loadDataSet(urn: ModURN)` and a method `urnsForStage(stageName: String)`. The `DataSetMgr` will resolve an `SmvDataSet` by name by asking its containing repo to load it, then recursively doing the same for each of the `SmvDataSet`s specified in the resolved `SmvDataSet's` dependency list.


# DataFrame Caching

SMV and Spark perform various types of caching to minimize calculations and also for resiliency in case of failure. One cache kept by SMV is of the `DataFrame` (or RDD) that each `SmvDataSet` resolves to. Without this cache, an `SmvDataSet`'s DAG would be recalculated every time it is resolved to an RDD.

Currently, the DataFrame cache for each `SmvDataSet` is internal to the `SmvDataSet`. This means that we lose its cache _each time it's reloaded_. This is acceptable under the assumption that the user reloads the `SmvDataSet` (by running the module dynamically) only when they have made changes and want to see the result. However, if the `SmvDataSet` will be reloaded every time it is resolved by name we will need to move the cache somewhere that is persistent when the `SmvDataSet` reloads so that the `DataFrame` can be reused if no changes have been made. The cache can be keyed using `SmvDataSet's` `versionedFqn`, which combines the FQN and `hashOfHash` of the `SmvDataSet`.


# External Dependencies and requiresDS()

Dependencies for an `SmvModule` are specified by the user through the module's `requiresDS()`. This method is then called internally e.g. in the `SmvModule`'s `doRun()`. If the user lists an `SmvExtModule` as a dependency, e.g. `requiresDS = Seq(SmvExtModule('a.b.c'))`, a new `SmvExtModule` is instantiated _every time_ `requiresDS()` is invoked - note that the user is in fact supplying a function, not a static value. This presents problems for our design:

### Problem 1:

Consider a simple dependency scenario
```
 x (s)
 |
 y (p)
```
where `x` is a Scala modules and `y` is a Python module. For simplicity's sake, assume neither of them is ephemeral. Suppose the user runs `x` through any entrypoint.

First, `SmvApp` asks `DataSetMgr` to load `'x'`. `DataSetMgr` delegates to `DataSetRepoScala` to load `x`, then asks `x` for its dependencies through `requiresDS()`, which returns a single `SmvExtModule` of `y`. Then, `DataSetMgr` delegates to `DataSetRepoPython` to load the `ISmvModulePy4J` for `y`, presumably wraps it in an `SmvExtModule` (the current wrapper for external modules) we will call `y1`, and returns `y1` to `DataSetMgr`.

Fast-forwarding a bit, `DataSetMgr` returns `x` to `SmvApp`, which runs `x` using `x.rdd()`. When `x` tries to compute its `DataFrame`, it will first try to read its cache on disk, invoking its `hashOfHash()` in the process. `hashOfHash()` recursively calls the `hashOfHash()` of each of `x`'s dependencies as discovered through `requiresDS()`, including a _new_ `SmvExtModule` `y2` of `y`. Invoking `y2`'s `hashOfHash()` causes `y2` to resolve its `ISmvModulePy4J` by asking `DataSetMgr` to load `'y'`.

Modules should be singletons, but `y` has now loaded twice. This will occur again when `x` resolves the RDDs of its dependencies. Also, each time this occurs, the entire dependency tree of `y` (which may be quite large) is reloaded.

Solution: For backwards compatibility's sake, we can't change how users specify dependencies. However, we can change how internals of SMV access a module's dependencies. We will differentiate between `requiresDS()`, which will remain a function defined by the user to specify a module's dependencies, and `resolvedrequiresDS`, the module's (fixed) canonical list of dependencies. `resolvedrequiresDS` would be set by a single call to `requiresDS()`. Thus we will neither instantiate new `SmvExtModule`s nor reload the external module when inspecting a module's dependencies.


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

Solution: Separate the declarative functionality of `SmvExtModule` needed by the user and the external module interface functionality needed internally. `SmvExtModule` will continue to be a declarative sugar for the user, but will no longer wrap `ISmvModulePy4J`. Instead, a new class `SmvExtModulePython` will take over this responsibility. `SmvExtModulePython` will be created only by `DataSetRepoPython`. Thus, although 2 `SmvExtModule`s of `y` are created when while loading `x`, neither of them forces a load of the Python module. In fact, `DataSetMgr` will load a `SmvExtModulePython` of `y` just once (using the same mechanism that prevents duplication of Scala modules) and this will be inserted in `x`'s `resolvedrequiresDS`. When `DataSetMgr` returns `x` to the app, its `resolvedrequiresDS` will include not an `SmvExtModule` but an `SmvExtModulePython`.


# Module Resolution

### Problem 1
Consider the dependency scenario
```
   x(s)
  /   \
y(p)  z(s)
```
where `x` and `z` are both Scala modules and `y` is a Python module.

When `DataSetMgr` loads `x`, it must ensure that the classes of `x`'s dependencies `y` and `z` are also loaded. Because of the way the Java `Classloader` work, when a `Classloader` loads the class of `x` it will also load the class of `x`'s Scala (but not Python) dependencies, and if `DataSetMgr` invokes `x.requiresDS()` it will create an instance of `z` and an instance of `SmvExtModule`; however, it will not create an instance of `y` on the Python side. Thus, we must ensure that `y` is loaded now, but `z` is not loaded a second time.

### Problem 2
`DataSetMgr` must facilitate `SmvModule` in loading its Python dependencies and resolving them as `SmvExtModulePython`s, but it is desirable that `SmvModule` know nothing about the `DataSetRepo`s or the `DataSetMgr`.

These 2 problems motivate the distinction of a new responsibility which we will term resolution of `SmvDataSet`s. Within a single `DataSetMgr.load`, to resolve an `SmvDataSet` is to

* Load its canonical class if it has not already been loaded in this transaction
* Resolve its dependencies and populate `resolvedrequiresDS`
* Return its canonical instance

By canonical class for an `SmvDataSet` we indicate the `SmvDataSet` class which is actually runnable, i.e. not an `SmvExtModule`. The canonical class of a subclass C of `SmvDataSet` is C itself _unless_ C is an `SmvExtModule`, in which case it is the `SmvExtModulePython` that corresponds to the same `SmvModule`.

`SmvDataSet`s are already well-suited to walk their own dependency trees recursively, and empowering `SmvDataSet`s to resolve their own dependencies while being agnostic of the mechanism to do so will also solve Problem 2. Therefore, `SmvDataSet` will provide a method `resolve(res: DataSetResolver)` that will resolve itself, where `DataSetResolver` is a class that loads and resolves for `SmvDataSet` each of its dependencies while keeping track of all `SmvDataSet`s loaded in this transaction. `DataSetResolver` can also be the entrypoint for `DataSetMgr` to load a module.

Since `DataSetResolver` tracks `SmvDataSet`s loaded in a transaction, each `DataSetMgr.load` will create a new `DataSetResolver`. `DataSetResolver` will provide 2 methods: `resolveDataSet(ds: SmvDataSet)` and `loadDataSet(fqn: String)`. `resolveDataSet` will simply call `ds`'s `resolve` unless it has already been resolved. `loadDataSet` will load the data set with that `fqn` from its repo, resolve it, and return the result.


# Repo Factories

Each transaction will need to create a new class loader, and will need to use that same class loader for every dataset loaded through `DataSetRepoScala`. Rather than store the class loader outside of `DataSetRepoScala` in any form, we will make all `DataSetRepo`s ephemeral so that each transaction creates a new set of each, and we will make each new `DataSetRepoScala` create its own class loader.

Creating a new `DataSetRepoScala` is trivial. To create a new `DataSetRepoPython`, we will need for the Python proxy to provide a factory for `IDataSetRepoPy4J`. Rather than registering a repo, the proxy will register a factory with the `SmvApp`.

We will create a new class hierarchy for repo factories. `DataSetRepoFactory` will be the generic repo factory interface. It will be implemented by `DataSetRepoPython` and `DataSetRepoScala`.


# runParams

Since `SmvExtModule`s for the same dataset will not be unique, `runParams` cannot be a simple map from `SmvDataSet` to `DataFrame` (the `SmvExtModule` used as a key by the user will not be the same created by `requiresDS()`). `runParams` will internally map `SmvDataSet` to `DataFrame` by name.


# Module links

Though we should never need to invoke `DataSetMgr.load` on a module link, we will encounter links when resolving the dependencies of modules. Pushing the linking logic into the repos would overcomplicate repos and risk loading the same module multiple times, so we will implement the linking logic in the `DataSetResolver`. In fact, we can carry it out similarly to how we do with `SmvExtModule`s. An `SmvModuleLink` should resolve to itself after resolving the module it links to. An `SmvExtModuleLink` will be a declarative sugar like `SmvExtModule`, resolving to a link to an `SmvExtModulePython` after loading the `SmvExtModulePython`. `SmvExtModulePython` is the only type of module that must resolve its dependencies from a list of URNs, so it will encapsulate the logic of differentiating between a link dependency and a direct dependency by parsing the URN. Finally, in order to link to a `SmvExtModulePython`, that `SmvExtModulePython` must be an output. The repo must specifically create an `SmvExtModulePython with SmvOutput` when `ISmvModulePy4J`'s `isOutput` is true.
