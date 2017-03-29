# SMV class loader

## Introduction
When developing modules, it is currently too arduous to compile/jar/restart the shell to see the effect of the new module.
Ideally, we should be able to "reload" a newly created/modified SmvModule without having to restart the shell, create a new context and re-cache the data.
The data cache issue can be taken care of by the Spark Server project which maintains a single spark context instance across invocations.  However, that solution does not address the issue of module modification as usually occurs during development.
The SMV class loader allows for dynamic reloading of client code (e.g. SmvModules).  This in turn reduces development cycle considerably.

## Configuration
The following parameters would be added to standard SmvApp config parameters.

* `smv.class_dir` : directory where SMV will look for new class instances.  Default to "./target/classes"

If `class_dir` is not specified, then SMV will revert to standard CLASSPATH lookup (using `Class.forName()` method) on default class loader.

If you use SBT to compile the project, you need to specify the "class_dir", typically need to add the
following to your `conf/smv-user-conf.props`.

```
smv.class_dir = ./target/scala-2.10/classes
```
