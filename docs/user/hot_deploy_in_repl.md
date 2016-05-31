# Hot Deploy in Spark Shell

## Motivation

A common iterative workflow used in developing SMV modules uses spark-shell for ad-hoc analysis and feedback; it typically involves the following steps:

1. Write the initial SMV module, build the fat jar with `mvn package` or `sbt assembly`
2. Start spark-shell with the assembled jar
3. Try out some ideas in the shell, then capture the code back into the module
4. Go back to step 1 and restart the shell

This is very similar to what people do in web-app development. And the pain point is similar: in web-app development, a lot of time is spent waiting for new code to be deployed; here, a lot of time is spent waiting for Spark to restart. Having the ability to hot-deploy code in the REPL would help a lot, just as being able to hot-deploy in containers like Play, or using JRebel, has helped boost productivity tremendously.

## Solution

We've patched Spark with the ability to hot deploy code.  This works well in conjunction with the ability to recompute SMV modules when change to the code is detected.  Currently the data server hosts the patched distribution for Spark 1.5.2 in ~/Public.

## Configuration

1. Configure the `smv.class_server.class_dir` property in $HOME/.smv/smv-user-conf.props or conf/smv-user-conf-props in the current SMV project.  By default this points `./target/classes` which is the target directory where mvn compiles the class files.  Change this to `./target/scala-2.10/classes` for use with sbt with `~compile`, and scala-2.10.
2. Run `spark-shell` with the following commandline from the current SMV project directory
```shell
spark-shell --master <MASTER> --conf 'spark.executor.userClassPathFirst=true' --conf 'spark.repl.dyncl.classdir=<CLASS_DIR>' --jars <FAT_JAR_PATH>
```
where the <CLASS_DIR> is the same value as `smv.class_server.class_dir`, such as `./target/scala-2.10/classes' and the <FAT_JAR_PATH> is the path to the assembled jar containing the SMV module.

## REPL

A new method `hotdeploy` has been added to `SparkContext` to allow Executors to create a new class loader so that changes in the code can be relaoded.  In `smv_shell_init.scala` the `ddf()` method will try to call this method if it's available before trying to resolve the SMV module. If you want to manually to trigger the classloader change, call `sc.hotdeploy` in the REPL.
