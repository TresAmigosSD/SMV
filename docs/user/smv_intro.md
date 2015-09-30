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
A single SMV application may contain one or more stages.  Each stage may contain one or more modules.  A stage corresponds to a single scala package that contains the modules.
By convention, each stage should define an `InputSet.scala` (or similarly named file) to enumerate all the inputs used in the given stage.
For example, given an app with 2 stages (stage1, stage2) we should have the following file layout

```
+-- src/main/scala/com/mycom/myproj
    +-- stage1
        +-- InputSetS1.scala
        +-- mod1.scala
        +-- mod2.scala
    +-- stage2
        +-- InputSetS2.scala
        +-- mod3.scala
        +-- ...
```

## Class Diagram
The class diagram of the SMV App below is provided as an FYI and is not critical to productively using SMV.
![SMV App Class Diagram](https://rawgit.com/TresAmigosSD/SMV/master/docs/design/app_class.svg)


