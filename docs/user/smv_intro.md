# SMV Application

## Introduction
SMV is a very simple data application framework that enables the following:

* Ability to group DataFrame operations into modules and organize the modules and their dependency in a hierarchical manner.
* No overhead of developers to use RDD and DataFrame to manipulate data and create new data set and variables
* Ability to use the same framework to quickly create ad-hoc analysis and the ad-hoc queries should be able to easily transform to production package
* Easy testing of module levels. Developer shouldn't have to create many repetitive and cumbersome scaffolding to test a module
* Intermediate data persistence and versioning. Data versioning is critical for applying test driven development on data application development
* Ability to test/run at the module level and at the application level. Developer should be able to test run modules they currently working on
* Same framework should also allow the creation of execution graphs at the application level.
* Provides utility to managing the end to end dependency graph, persisted data meta info and execution logging

## SmvDataSet

We call our library Spark Modularized View because its objective is to modularize data applications. This modularity is captured in `SmvDataSet`. An `SmvDataSet` is a sequence of Spark `DataFrame` operations which takes multiple input `DataFrame`s and generates a single `DataFrame`. Since each `SmvDataSet` only has a single `DataFrame`, we can bind an `SmvDataSet`'s code with its output. On the other hand, all the `DataFrame`s an `SmvDataSet` depends on can be represented by the `SmvDataSet` which generated them. Therefore, the entire data flow can be organized as a collection of `SmvDataSet`s which form a directed graph.

## Application Hierarchy
A single SMV application contains one or more stages. A stage is a user-specified package name which exists in the user's code. Each stage contains one or more `SmvDataSet`. Each `SmvDataSet` must belong to a stage to be discovered by the application. By convention, the input `SmvDataSet`s for each stage should reside in a sub-package called `input`. For example, given an Scala app with a stage called `etl` would be structured like
```
+-- src/main/scala
    +-- etl
        +-- input
            +-- package.scala
        +-- module.scala
    ...
```
and a similar Python project would be structured like
```
+-- src/main/python
    +-- etl
        +-- input.py
        +-- module.py
    ...
```
