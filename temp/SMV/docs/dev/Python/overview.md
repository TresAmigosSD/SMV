# Background

Spark, specifically PySpark, uses py4j to provide access to JVM to code written in Python.  The Pyspark classes, such as DataFrame and Column, all follow the proxy pattern and forward calls to their JVM counterpart through the py4j gateway.  The returned results from the JVM are then wrapped and returned as Pyspark classes.

We follow the same design when it comes to invoking methods on JVM objects from the Python side.

However, for Python SmvModules to work seamlessly with Scala SmvModules, JVM objects would sometimes need to invoke methods on Python objects as well.  In particular, `SmvApp` would need to be able to call `doRun` on Python `SmvDataSet` subclasses.

To accomplish this task, we rely on the callback server that's part of the py4j library.

# Making Scala and Python Work Together

The callback feature in the py4j library allows a Python class to implement any number of Java interfaces.  An object in the JVM that receives such a Python object (internally through Java's dynamic proxy) can then invoke methods defined in the Java interface and implemented by the Python object.

We define an interface `SmvDataSetRepository` (TODO: may need to come up with a better name, DataSetManager, Handler, Pool, etc) to encapsulate the common operations that can be performed with and upon `SmvDataSet`s.

# What the Interface Provides

Through the interface you can make the following queries:

* `hasDataSet(modUrn)`       : does it contain the named `SmvDataSet`?
* `isEphemeral(modUrn)`	     : does the result of the `SmvDataSet` need to be persisted?
* `dependencies(modUrn)`     : what other datasets does the `SmvDataSet` depend on? (returns a csv of urns)
* `datasetHash(modUrn, sup)` : the dataset hash (by default include all its superclasses)
* `outputModsForStage(stage)`: what are the output modules for a given stage (returns a csv of urns)

The interface also defines the following operations:

* `dqmWithTypeSpecificPolicy(modUrn)` : returns the `DQM` policy for the named `SmvDataSet`
* `getDataFrame(modUrn, dqmValidator, fqn~>dataframe)` : returns the result of running the `SmvDataSet`
* `rerun(modUrn, dqmValidator, fqn~>dataframe)` : re-run the `SmvDataSet` after code change

# Changes to SmvApp

The original `resolveRDD` method, which takes an `SmvDataSet` and returns a `DataFrame`, is replaced with a `runModule` method, that takes the name of the `SmvDataSet` and a sequence of `SmvDataSetRepository`s and returns the `DataFrame` result from running the named `SmvDataSet` (TODO: this may be refactored so that at the start of the `SmvApp`, all implementing repositories are registered, then the `runModule` method would simply search through the known repositories for one that knows about the named `SmvDataSet`).

Within the `runModule` method, the owning repository is queried for the dependencies of the named `SmvDataSet`, and each dependency is recursively passed to `runModule` before the named `SmvDataSet` is run.

Cross-stage (`SmvModuleLink`) - as well as cross-language (`SmvExtDataSet`) - dependencies are also resolved to their appropriate owning repository and cannonical names (in the form of `urn:smv:mod:<fqn>`) in the `runModule` method.

# Naming SmvModules

# Scala Implementation

# Python Implementation

# Changes to SmvDataSet
