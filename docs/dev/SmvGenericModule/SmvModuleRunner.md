# General SmvDataSet Running Logic and Flow

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
A -> B
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
* `doRun()`: method provided by a concrete sub-class of `SmvDataSet`, which returns the DataFrame

Currently `smvApp.runModule` method is pretty light, which just call `B.rdd()` with attaching a `RunInfoCollector`, and return the result from `B.rdd()` and the collector.

## Custom metadata, Ephemeral and DataFrame Caching

For Pandas DF, or any other output which are in memory, the `Ephemeral` concept and `persist` concept are so easy 
to understand and manage. So basically,

* Persist - save the data to some storage engine (disc, db, cloud, etc.)
* Ephemeral - don't do persist on this module

Even for an `Ephemeral` module, user should still be able to use it whatever ways they like. 

In the simple case (in memory DF), the result from `doRun` and cached in the `rdd` method buffer has all the data 
ready for caller to use and to use multiple times, regardless whether it's Ephemeral or persisted.

The logic within `computeDataFrame` is very simple:
```python
if (self.isEphemeral()):
    df = self.doRun(...)
else:
    if (data_persisted):
        df = getPersistedData()
    else:
        df = self.doRun(...)
        persistData(df)

if (meta_persisted):
    meta = getPersistedMeta()
else
    usermeta = self.metadata(df)
    meta = create_system_meta + usermeta
    persistMeta(meta)

return df
```

For Spark DataFrame, since it has this delay execution feature, the result from `doRun` method is actually just a
"recipe", called plan, of how to calculate the data. Because of that, we should reuse the persisted data for the 
non-ephemeral` case, instead of re-run the plan. So the logic changes to:
```python
if (self.isEphemeral()):
    df = self.doRun(...)
else:
    if (data_persisted):
        df = getPersistedData()
    else:
        raw_df = self.doRun(...)
        persistData(df)
        df = getPersistedData()

if (meta_persisted):
    meta = getPersistedMeta()
else
    usermeta = self.metadata(df)
    meta = create_system_meta + usermeta
    persistMeta(meta)

return df
```

Above logic solves the potential re-run of the plan for non-ephemeral module. However there is danger left for the 
ephemeral modules. Consider the following case,

```
A -> B -> C
```

With `A` and `B` are ephemeral, and both have a some user metadata method which perform some action on the result DF.

When call `runModule('C')`, B's `computeDataFrame` is called, and it returns B's DF to C, and C's run method used B's DF. 
Also, since B has a usermeta, so B's DF have to have an action there too. So B's DF plan has to run twice, one for the 
usermeta, and the other for C's calculation. What even worse is that, each of B's DF's action will cause A's action too. 
So basically A's DF has to run 3 times, one for its own usermeta, one for B's usermeta, and one for C's calculation. 

So each usermeta not only add an action to the c