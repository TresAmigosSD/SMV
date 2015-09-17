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
A single SMV application may contain one or more stages.  Each stage may contain one or more packages which in-turn can contain one ore more modules.
While SMV does not enforce any file/source level hierarchy, it is recommended that the source file hierarchy matches the stage/package/module hierarchy.
For example, given an app with 2 stages (stage1, stage2) and each stage with 2 packages (pkg1,2,3,4), we should have the following file layout

```
+-- src/main/scala/com/mycom/myproj
    +-- stage1
        +-- input
        +-- pkg1
        +-- pkg2
    +-- stage2
        +-- input
        +-- pkg3
        +-- pkg4  (package FQN = com.mycom.myproj.stage2.pkg4)
```

A couple of things to note about the above file hierarchy

* Following standard java/scala package name convention, the packages above will have their parent stage name in their name.
* Each stage has an `input` package for defining the inputs into the stage.

TODO: add class diagram to show class relationship below!!!
Design Diagram
![Alt text](https://rawgit.com/TresAmigosSD/SMV/master/docs/appFramework.files/appFramework.svg)

