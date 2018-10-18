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

## The Simple Logic 

For Pandas DF, or any other output which are in memory or get calculated anyhow,
any time, the `Ephemeral` concept and `persist` concept are so easy to understand
and manager. So basically,

* Persist - save the data to some storage engine (disc, db, cloud, etc.)
* Ephemeral - don't do persist on this module

Even for an `Ephemeral` module, user should still be able to use it whatever
ways they like. 

For easy comparison, let's use current `SmvDataSet` implementation methods for
some pseudo code. 

* doRun - method a concrete `SmvDataSet` to provide to return a `Result` (currently a DF)
* computeDataFrame - `SmvDataSet` private method, which calls `doRun`, and manages metadata calculation & persisting
* rdd - `SmvDataSet` method to maintain an in-memory cache for the Results calculated by `computeDataFrame` 

In the simple case (in memory DF), whatever cached in the `rdd` method buffer are
the results, which can be used by user within a single transaction without need to
re-calculate, regardless whether it's Ephemeral or persisted.

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

return (df, meta)
```

Let's see for Spark type of delayed action data, what need to be changes above, 
for now we ignore the Dqm part. The only thing need to change is to use the 
read-back `df` instead of the raw df when there is a persisting:
```python
if (self.isEphemeral()):
    df = self.doRun(...)
else:
    if (data_persisted):
        df = getPersistedData()
    else:
        raw_df = self.doRun(...)
        persistData(raw_df)
        df = getPersistedData()

if (meta_persisted):
    meta = getPersistedMeta()
else
    usermeta = self.metadata(df)
    meta = create_system_meta + usermeta

return (df, meta)
```

The only danger there is a `Ephemeral` module with a non-trivial `metadata` method.
In that case, the data have to pass through the logic in this modules `doRun` 
generated plan 2 times. 