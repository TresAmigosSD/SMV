# Run SmvGenericModule with Visitor Pattern

As part of the effort of making SmvDataSet more generic, we need to generalize the module running part also.

The goal of generalize `SmvDataSet` to `SmvGenericModule` is to support 
* Different type of module result types, such as 
    - Spark DataFrame
    - Spark Dataset
    - Pandas DataFrame
    - H2O DataFrame
    - Python object (pico-able or not)
    - custom output types
* Different type of persisting strategies, even just for Spark DataFrame to support
    - Csv on HDFS
    - on Hive
    - Parquet on HDFS
    - ORC on HDFS
    - custom persisting strategies

The design of `SmvIoStrategy` covers how we will handle the persisting variations 
of `SmvGenericModule`. This document is focusing on the variations of the running logic of different types of results, and how to generalize.

## Current Run Module Logic Flow

We will focus on the `runModule` method of `SmvApp` class for this design, since we will 
have a SINGLE method to run a or multiple modules in an App. From smv-run command line, 
smv-shell command, smv-server Api, or from custom driver script, all use the same single 
method of `SmvApp` to run modules. In this document just consider the `runModule` method 
is the SINGLE entry.

Since `runModule` method is the single entry, we can consider its scope as a **Transaction**
for module running. Please don't be confused with the module resolving transaction, which 
is defined in the `TX` class, which is just used for module resolving (create module instances
from classes).

Consider a simple App:
```
A <- B
```

When we call `runModule('B')`, the flow of method calls are:

* `smvApp.runModule('B')` =>
* `B.rdd(...)` =>
* `B.computeDataFrame(...)` =>
* `B.doRun(...)` =>
    - `A.rdd(...)` =>
    - `A.computeDataFrame(...)` =>
    - `A.doRun(...)`

The 3 methods of `SmvDataSet` are for the following things:

* `rdd()`: interface method to run a given DS, maintain an app level DF cache, so will not call the same `computeDataFrame()` twice in the life cycle of the entire app. Please note that the cache is index by versioned-FQN, so in interactive mode (smv-shell or smv-server), the DS with same FQN might have multiple versioned-FQNs
* `computeDataFrame()`: apply logics of `isEphemeral`, persisting, runInfo (metadata) collection etc, while calling `doRun` to really create the DF
* `doRun()`: method provided by a concrete sub-class of `SmvDataSet`, which returns the DataFrame by recursivly call the `rdd` method of the modules it depends

Currently `smvApp.runModule` method is pretty light, which just call `B.rdd()` with attaching a `RunInfoCollector`, and return the result from `B.rdd()` and the collector.

## Challenges of generalize current flow

The main challenge is Spark DataFrame's `lazy eval` feature and the `DQM` tool we introduced. Basically
for any ephemeral module, the `DQM` can't be validated within the `computeDataFrame` method of itself, unless we force an action on the result DataFrame.

For generic data types, there could be other dataframes which take the `lazy eval` approach and have 
similar problem as Spark DataFrame. 

To solve that, we need to delay the validation of `DQM` to after the earliest action on that DF. To 
generalize that idea, we can introduce a `SmvGenericModule` method `postAction()`. In the 
`SmvSparkModule` case, that `postAction()` method will do the `DQM` validation and necessary metadata
updateing.

Then the question comes to where to call this `postAction()` method. There are multiple cases:
* For non-ephemeral module, call after data persisting within the `computeDataFrame` methods
* For ephemeral module, delay to after the first downstream non-ephemeral module's persist action
* For ephemeral module which does not have a downstream non-ephemeral module, force an action, at the `runModule` method level, and run `postAction()`

Also we need to make sure the `postAction()` only run once per transaction.

Using current run-flow to implement above will be too complicated and ugly.

**BELOW NOT FULLY UPDATED**
---------------------------------------------------------

## Using Visiter Pattern to Run Module

To keep it simple, we start with implementing a simple run without the complexity
of `lazy eval`, meta, or `DQM`.

### Related Methods in SmvGenericModule
```python
class SmvGenericModule:
    ...
    def __init__(self):
        self.resolvedRequiresDS = []
        ...

    def resolve:
        ...
    
    def rdd(self):
        if (self.versioned_fqn not in app.dfCache):
            # by the time this module's rdd is called, all the ancestors are cached
            app.dfCache.update({self.versioned_fqn, self.computeDataFrame()})
        res = app.dfCache.get(self.versioned_fqn)
        return res

    def computeDataFrame(self):
        ...
        df = self.doRun()
        ...
        return df
    
    def doRun(self):
        i = self.build_RunParams(app.dfCache)
        return self.run(i)
```

### ModulesVisitor
```python
class ModuleVisitor(object):
    def __init__(self, roots):
        self.queue = self._build_queue(roots)

    ...
    def dfs_visit(self, action, state):
        for m in self.queue:
            action(m, state)
    
    def bfs_visit(self, action, state):
        for m in reversed(self.queue):
            action(m, state)
```

### Run module
```python
def runModule(mods):
    def runner(mod, dummy):
        mod.rdd()
    ModulesVisitor(mods).dfs_visit(runner, None)
    return [app.dfCache.get(m.versioned_fqn) for m in mods]
```

So basically the `doRun` method will not try to run the modules depends,
instead, the `runModule` method walk through the graph using the `visit` method.

## How the visitor pattern solves our problems

### Create a module list for each transaction

To guarantee each module's `postAction` method run and only run once per transaction, we need to manage a 
"modules-to-run" list in `runModule` method. We can use the `visitor` to create that list:

```python
def runModule(mods):
    mods_to_run_postAction = set(ModulesVisitor(mods).queue)
    ...
```

### Implement computeDataFrame to use mods_to_run_postAction

The `computeDataFrame` method need to call `postAction` method and update `mods_to_run_postAction`.

Consider the the simple case where all modules are non-ephemeral:
```python
class SmvGenericModule:
    ...
    def rdd(self, run_set):
        if (self.versioned_fqn not in app.dfCache):
            app.dfCache.update({self.versioned_fqn, self.computeDataFrame(run_set)})
        else:
            run_set.discard(self)
        res = app.dfCache.get(self.versioned_fqn)
        return res

    def computeDataFrame(self, run_set):
        df = self.doRun()
        self.persistData(df)
        if (self in run_set):
            self.postAction()
            run_set.discard(self)
        return df
    ...
```

and 

```python
def runModule(mods):
    mods_to_run_postAction = set(ModulesVisitor(mods).queue)
    def runner(mod, run_set):
        return mod.rdd(run_set)
    ModulesVisitor(mods).dfs_visit(runner, mods_to_run_postAction)
    return [app.dfCache.get(m.versioned_fqn) for m in mods]
```


### Run postAction as early as possible

The ideal place for running the `postAction` method of a module, is just after the DF generated by the module 
firstly used (has an action). There could be the following cases:

* Module itself get persisted 
* User metadata calculation could (or could not) cause an action
* Some forced actions applied
* For Ephemeral module, any downstream modules action cause current module's action

From `runModule` method, there will be 4 visits total:
* Create `mods_to_run_postAction`
* Calculate `df` (will persist the non-ephemeral modules)
* Calculate user metadata 
* If still modules left in `mods_to_run_postAction` force an action on them and run `postAction`

To attach dqm tasks, we just generalize it to a `preAction` method. Adding up all, the relevant methods are

```python
class SmvGenericModule:
    ...
    def run_ancestor_and_me_postAction(self, run_set):
        def run_delayed_postAction(mod, _run_set):
            if (mod in _run_set):
                mod.postAction()
                _run_set.discard(mod)
        ModuleVisitor(self).dfs_visit(run_delayed_postAction, run_set)

    def computeDataFrame(self, run_set):
        raw_df = self.doRun()
        df = self.preAction(raw_df) # attach dqm task here 
        
        if (self.isEphemeral):
            return df
        else:
            self.persistData(df)
            self.run_ancestor_and_me_postAction(run_set)
            return self.unPersistData()

    def calculate_user_meta(self, run_set):
        df = app.dfCache.get(self.versioned_fqn)
        self.metaCache.addUserMetadata(self.metadata(df))
        if (self.hadAction()):
            self.run_ancestor_and_me_postAction(run_set)

    def forcePostAction(self, run_set):
        if (self in run_set):
            df = app.dfCache.get(self.versioned_fqn)
            self.force_an_action(df)
            self.run_ancestor_and_me_postAction(run_set)
```
Note: we used a `hadAction` method. For `SmvSparkModule`, the `DQMvalidator` has the info on whether the accumulator has
non-zero record count. If the count is non-zero, `hadAction` is True. However there is a chance of
false-negative, in case that the DF just has zero records. If possible may need a better way to set
this flag. For `SmvGenericModule`, `hadAction` always return True (default to no lazy-eval case).

In case of false-negative, the current ephemeral module and its ancestor ephemeral modules may have 
action twice and cause double counting in the DQM state. Given the DQM state could anyhow double counted
because of fault-tolerance nature of RDD, this small chance of double counting should be acceptable.

And in `runModule`:
```python
def runModule(mods):
    ...
    mods_to_run_postAction = set(ModulesVisitor(mods).queue)
    def runner(mod, run_set):
        return mod.rdd(run_set)
    ModulesVisitor(mods).dfs_visit(runner, mods_to_run_postAction)

    def run_meta(mod, run_set):
        return mod.calculate_user_meta(run_set)
    ModulesVisitor(mods).dfs_visit(run_meta, mods_to_run_postAction)

    # If there are still leftovers, force run
    if (len(mods_to_run_postAction) > 0):
        def force_run(mod, run_set):
            mod.forcePostAction(run_set)
        ModulesVisitor(mods).bfs_visit(force_run, mods_to_run_postAction)
    
    return [app.dfCache.get(m.versioned_fqn) for m in mods]
```

Please note that for the `force_run` visiting, we used `bfs_visit` (breadth-first) method. The reason is that
the `forcePostAction` will add an action to the module. Since downstream modules action also caused upstream modules
action, we'd rather start the force action from the later modules.

## Some other details

### Persist Meta
Since we also need to persist metadata for each module, need another visit to all the modules,

```python
class SmvGenericModule:
    ...
    def persist_meta(self):
        persistMeta(self.metaCache)
        return unPersistMeta()
```

```python
def runModule(mods):
    ...
    all_meta = {}
    def persist_meta(mod, all_meta):
        all_meta.update({mod.fqn: mod.persist_meta()})
    ModulesVisitor(mods).dfs_visit(persist_meta, all_meta)

    metas = [all_meta.get(m.fqn) for m in mods]

    return zip(dfs, metas)
```
    
### Quick Run

A quick-run on a module means:

* Use persisted data as possible
* Never persist new data
* No user-meta
* No DQM

So need to add `is_quick_run` flag to `rdd` and `computeDataFrame`. 
```python
def computeDataFrame(self, run_set, is_quick_run):
    raw_df = self.doRun()
    df = self.preAction(raw_df) # attach dqm task here 
    
    if (self.isEphemeral):
        return df
    elif (is_quick_run):
        try:
            res = self.unPersistData()
        except:
            res = df
        return res
    else:
        self.persistData(df)
        self.run_ancestor_postAction(run_set)
        return self.unPersistData()
```

And create a `quickRunModule` method, 
```python
def quickRunModule(mods):
    ...
    def runner(mod, dummy):
        return mod.rdd(run_set, [], is_quick_run=True)
    ModulesVisitor(mods).dfs_visit(runner, [])

    return [app.dfCache.get(m.versioned_fqn) for m in mods]
```

### MetaHistory, SmvRunInfo, SmvRunInfoCollector
Current implementation:

* `MetaHistory`: collect module's meta from last N multiple runs, which could across multiple versions (hashOfHash), and persisted to file as JSON
* `SmvRunInfo`: just a tuple of `(meta, metaHistory)`
* `SmvRunInfoCollector`: a list of all the SmvRunInfos generated from one run-transaction

All those should belong to the runner, instead of part of the `SmvGeneralizedModule`. 

## SmvModuleRunner

Since the `runModule` getting pretty heavy, let's create `SmvModuleRunner` class, which represents a run-transaction. The `runModule` and 
`quickRunModule` methods in `SmvApp` will become interface method to access `SmvModuleRunner`. 

For `smv-shell`, should consider to always use `quickRunModule` for `df`, and introduce a different method to `run` with `runModule`. 
