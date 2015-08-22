# Running SMV Application

An SMV application can be run using the standard "spark-submit" method or run using "spark-shell"

## Run SMV Application using spark-submit

The format of the "spark-submit" call is:
```shell
$ spark-submit [standard spark-submit-options] --class org.tresamigos.smv.SmvApp [options] [what-to-run]
```

<br>
<table>

<tr>
<th colspan="3">Options</th>
</tr>

<tr>
<th>Option</th>
<th>Default</th>
<th>Description</th>
</tr>

<tr>
<td>--smv-props</td>
<td>None</td>
<td>allow user to specify a set of config properties on the command line.
<br>
<code>$ ... --smv-props "smv.stages=s1"</code>
<br>
See <a href="appConfig.md">Application Configuration</a> for details.
</td>
</tr>

<tr>
<td>&#8209;&#8209;smv&#8209;app&#8209;conf</td>
<td>conf/smv-app-conf.props</td>
<td>option to override default location of application level configuration file.</td>
</tr>

<tr>
<td>&#8209;&#8209;smv&#8209;user&#8209;conf</td>
<td>conf/smv-user-conf.props</td>
<td>option to override default location of user level configuration file.</td>
</tr>

<tr>
<td>--edd</td>
<td>off</td>
<td>toggle edd creation on/off.
<br>
When enabled, all persisted modules will have a corresponding EDD file that shows some standard statistical information about the result.
</td>
</tr>

<tr>
<td>--graph / -g</td>
<td>off</td>
<td>Generate a dependency graph ".dot" file instead of running the given modules.<br>
graphvis must be used to convert the ".dot" file to an image or doc.  For example:<br>
<code>$ dot -Tpng com.foo.mod.dot -o graph.png</code>
</td>
</tr>

<tr>
<td>--json</td>
<td>off</td>
<td>Generate a json file of the provided modules and their dependencies.</td>
</tr>

<tr>
<th colspan="3">What To Run
<br>
One one of the options below must be specified.
</th>
</tr>

<tr>
<th colspan="2">Option</th>
<th>Description</th>
</tr>

<tr>
<td colspan="2">--run-module mod1 [mod2 ...] / &#8209;m</td>
<td>Run the provided list of modules directly (even if they are not marked as SmvOutput module)
</td>
</tr>

<tr>
<td colspan="2">--run-stage stage1 [stage2 ...] / &#8209;s</td>
<td>Run all output modules in given stages.
</td>
</tr>

<tr>
<td colspan="2">--run-app </td>
<td>Run all output modules in all configured stages in current app.
</td>
</tr>

</table>

## Run Spark Shell with SMV

We can pre-load SMV jar when run spark-shell.

```shell
$ spark-shell --executor-memory 2g --jars ./target/smv-1.0-SNAPSHOT.jar -i sparkshellinclude.scala
```
where `sparkshellinclude.scala` will be loaded for convenience. It could look like the follows,

```scala
import org.apache.spark.sql._, functions._
import org.tresamigos.smv._
val sqlContext = new SQLContext(sc)
import sqlContext.implicits._
```

Or you can use the existing script under ```shell``` directory.
```shell
./shell/run.sh
```
You may need to modify the script a little for your own environment.
You can put utility functions for the interactive shell in the ```shell_init.scala``` file.

Please note that the Spark Shell should in the same version as the SMV build on. Current version
SMV uses Spark 1.3.0, so you need the spark-shell in 1.3.0 package.

