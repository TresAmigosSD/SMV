# SMV class loader client/server

## Introduction
When developing modules, it is currently too arduous to compile/jar/restart the shell/server to see the effect of the new module.
Ideally, we should be able to "reload" a newly created/modified SmvModule without having to restart the server, create a new context and re-cache the data.
The data cache issue can be taken care of by the Spark Server project which maintains a single spark context instance across invocations.  However, that solution does not address the issue of module modification as usually occurs during development.
The SMV class loader allows for the serving of class data using a client/server architecture and utilizing a custom class loader on the client side (shell/server).  This allows for dynamic reloading of client code (e.g. SmvModule/SmvFile/utility classes).  This in turn reduces development cycle considerably.

## Configuration
The following parameters would be added to standard SmvApp config parameters.

* `smv.class_server.host` : host name of class server.  If this is specified, then the dynamic class loader server will not be utilized and SMV will look for module classes on the server
* `smv.class_server.port` : port number where class server will be listening for connections and where SmvApp will connect to.  Defaults to 9900.
* `smv.class_server.class_dir` : directory where the class server would look for new class instances.  If this is specified but host was not specified, then the dynamic class loader is run in standalone mode (that is, a server instance is not required and the new class files are looked up directly from this app).  Default to "./target/classes"

If neither `host`, nor `class_dir` are specified, then SMV will revert to standard CLASSPATH lookup (using `Class.forName()` method) on default class loader.

Although SMV class loader designed to use client/server structure, in most cases, we don't need to start
the server at all. Since we typically run `smv-shell` command with `--master local[*]` mode or
`--master yarn-client` mode, in both cases the master node is the same as the machine you launch the job,
which means that the client and server of the SMV class loader are on the same machine. In this case
SMV will be smart enough to bypass the network communication and "just works". From a user point of view,
as long as you run `smv-shell` on local or yar-client mode, you don't need to specify
`smv.class_server.host`, or `smv.class_server.port`. Also since the default value of
`smv.class_server.class_dir` is the default place Maven put the compiled classes, as long as you use Maven
to compile the project, you don't need to specify "class_dir" either.

If you use SBT to compile the project, you need to specify the "class_dir", typically need to add the
following to your `conf/smv-user-conf.props`.

```
smv.class_server.class_dir = ./target/scala-2.10/classes
```

## Running from shell
Once the SMV shell is launched through the standard method, it is possible to utilize the "ddf" function to reload and rerun a module.  This can be done **without** exiting the shell first and rebuilding the app as usual.  The example below uses an app directory created using the `smv-init` script.

```scala
scala> val r = ddf("com.myapp.stage2.StageEmpCategory")
r: org.apache.spark.sql.DataFrame = [ST: string, EMP: bigint, cat_high_emp: boolean]

scala> r.collect
res2: Array[org.apache.spark.sql.Row] = Array([50,245058,false], ...)
```

In another window, the code for the `StageEmpCategory` module is modified and compiled (no need to rebuild the app fat jar, just recompile)
```shell
$ mvn compile
```
This will generate a new class under the `target/classes` directory.

In the same shell as before, rerun the ddf command:
```scala
scala> val r2 = ddf("com.myapp.stage2.StageEmpCategory")
r2: org.apache.spark.sql.DataFrame = [ST: string, EMP: bigint, cat_high_emp: boolean, cat_high_emp2: boolean]

scala> r2.collect
res3: Array[org.apache.spark.sql.Row] = Array([50,245058,false,true], ...)
```
Note that the new result has the newly added column.  Both results exist at the same time and can be compared against each other.


## Running Server
In order to utilize the SMV class server in a cluster environment, the server instance must be started.  A convenience script is provided in `SMV/tools` directory to launch the server.  The server **MUST** be launched from the top level of an application directory.

```shell
$ ${SMV_HOME}/tools/smv-class-server
```

**Note:** the above is only needed if running on a cluster.

## Dynamic module loading with SBT continuous compile
SMV and projects created from `smv-init` command can be compiled using either Maven (the `mvn` command) or
SBT (the `sbt` command).

One big advantage of SBT is that it support continuous compile. When you start the continuous compile
SBT will detect code changes and try to re-compile changed classes automatically. With the continuous
compile feature of SBT, the SMV class loader's dynamic module loading feature becomes more convenience!

The only difference is the you need to specify `smv.class_server.class_dir` in the
smv-user-conf.props file, since SBT put the compiled class files in a different dir:
```
smv.class_server.class_dir = ./target/scala-2.10/classes
```

Basically as long as you make changes in the code and save the file, the new module can be resolved by
`ddf` command in the running smv-shell already. (Depend on the compile speed, typically in 1-5 seconds,
the new module will be ready to use in shell.)

SBT command is slightly different from Maven commands, here is a very simple comparison, for details,
please look for some basic SBT document:
* Package: `sbt package`
* Install: `sbt publish-local`
* Package app with the fat-jar: `sbt assembly`
* Compile: `sbt compile`
* Continuous compile: `sbt '~ compile'`
