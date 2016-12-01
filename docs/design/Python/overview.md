# Background

Spark, specifically PySpark, uses py4j to provide access to JVM to code written in Python.  The Pyspark classes, such as DataFrame and Column, all follow the proxy pattern and forward calls to their JVM counterpart through the py4j gateway.  The returned results from the JVM are then wrapped and returned as Pyspark classes.

We follow the same design when it comes to invoking methods on JVM objects from the Python side.

However, for Python SmvModules to work seamlessly with Scala SmvModules, JVM objects would sometimes need to invoke methods on Python objects as well.  In particular, `SmvApp` would need to be able to call `doRun` on Python `SmvPyDataSet` subclasses.

To accomplish this task, we rely on the callback server that's part of the py4j library.

# Making Scala and Python Work Together

The callback feature in the py4j library allows a Python class to implement any number of Java interfaces.  An object in the JVM that receives such a Python object (internally through Java's dynamic proxy) can then invoke methods defined in the Java interface and implemented by the Python object.

We define an interface `SmvDataSetRepository` (TODO: may need to come up with a better name, DataSetManager, Handler, Pool, etc) to encapsulate the common operations that can be performed with and upon `SmvDataSet`s.

# What the interface Provides

# Naming modules

# Scala Implementation

# Python Implementation

# Changes to SmvApp

# Changes to SmvDataSet

