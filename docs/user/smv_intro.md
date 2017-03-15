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

## Application Hierarchy
A single SMV application contains one or more stages. A stage is a user-specified package name which exists in the user's code. Each stage contains one or more modules. Each module must belong to a stage to be discovered by the application. By convention, the input datasets for each stage should reside in a sub-package called `input`. For example, given an Scala app with a stage called `etl` would be structured like
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

Within a stage, there are multiple `SmvDataSet`s.

At the top level there are 2 types of `SmvDataSet`
* `SmvFile`, and
* `SmvModule`

`SmvFile`s link to physical files stored in HDFS. Pleas see [SMV Files](smv_file.md) for details.
`SmvModule`s are datasets created in by the code. They could depends on other `SmvDataSet`s.

`SmvModule` could be mixed with `SmvOutput`, which labeled it to be an output dataset of a stage.
Without the `SmvOutput` label, a `SmvModule` will be considered intermediate dataset.
Please see [SMV Modules](smv_module.md) for more details.

A special type of `SmvModule` is `SmvModuleLink`. It is a link to other `SmvModule`. For example,
`stage1` has `mod2` as a `SmvOutput`, and `stage2` uses it as input. Instead of refer `stage1.mod2`
directly in `stage2`, a `SmvModuleLink` will be created in the `input` sub-package and link to
`stage1.mod2`. The pairs of `SmvOutput` and `SmvModuleLink` defines the interface between stages.
Please see [SMV Stages](smv_stages.md) for more details.
