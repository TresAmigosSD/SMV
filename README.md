# Spark Model Variables (SMV)
Spark Model Variables enables users to quickly build model variables on Apache Spark platform.
Please refer [FAQ](docs/FAQ.md) for details.

## Build
To build this package, use maven as follows:
```shell
$ mvn clean install
```
You must use maven version 3.0.4 or newer to build this project. 
In some system, instead of comand `mvn`, you may need to use `mvn3`.

With Spark version 1.3, you need to set the Maven memory to avoid compile failure  
```shell
export MAVEN_OPTS="-Xmx512m -XX:MaxPermSize=128m"
```

Current version depend on Spark 1.3.0. It should also works under 1.3.1. Please modify 
`pom.xml` to change Maven dependency configuration on Spark if you want to compile it with 
Spark 1.3.1 library. 

For more details of setting up the system from scretch, please refer 
[Smv Installation](docs/SMV-Installation.md).
It was written for earlier SMV version with Spark 1.1. Although the latest version 
of SMV actually work with Spark 1.3, the document could still be helpful for the overall 
process.

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

## Run SMV Application using spark-submit

SMV applications can be run using standard spark submit.
```shell
$ spark-submit [standard spark-submit-options] --class SmvApp [options] [what-to-run]
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
See <a href="docs/appConfig.md">Application Configuration</a> for details.
</td>
</tr>

<tr>
<td>--smv-app-conf</td>
<td>conf/smv-app-conf.props</td>
<td>option to override default location of application level configuration file.</td>
</tr>

<tr>
<td>--smv-user-conf</td>
<td>conf/smv-user-conf.props</td>
<td>option to override default location of user level configuration file.</td>
</tr>

<tr>
<td>--dev / -d</td>
<td>off</td>
<td>toggle development mode on/off.
<br>
In development mode, all intermediate modules are persisted and versioned not only the output modules.
</td>
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
<td colspan="2">--run-module mod1 [mod2 ...] / -m</td>
<td>Run the provided list of modules directly (even if they are not marked as SmvOutput module)
<br>Note: Not implemented yet.
</td>
</tr>

<tr>
<td colspan="2">--run-stage stage1 [stage2 ...] / -s</td>
<td>Run all output modules in given stages.
<br>Note: Not implemented yet.
</td>
</tr>

<tr>
<td colspan="2">--run-app / -a</td>
<td>Run all modules in all configured stages in current app.
<br>Note: Not implemented yet.
</td>
</tr>

</table>


## Full example for ad hoc data discovery

```scala
val sqlContext = new SQLContext(sc)
import sqlContext._

val srdd = sqlContext.csvFileWithSchema("/data/input", "/data/input.schema")
val mini_srdd = srdd.select($"tx_id", $"tx_amt", $"tx_date", $"tx_type")

// create EDD base tasks (see description under EDD section below)
val edd = mini_srdd.edd.addBaseTasks()

// add histogram to enumerated "amount" types.
edd.addAmountHistogramTasks("tx_amt")

// add generic histogram calcuation for given fields.
edd.addHistogramTasks("tx_date", "tx_type")()

// generate the histogram and save it.
edd.saveReport("outreport")
```

## Ad Hoc Data Discovery VS. App Development 
The nature of Data Application development is the circle of Data Discovery -> Variable Coding -> Data Discovery. 
SMV is designed to support seamlessly switching between those 2 modes. 

In a nutshell, SMV extends Spark Shell with additional function and components to provide an interactive environment 
for ad hoc Data Discovery; Also the SMV provides an [Application Framework](docs/appFramework.md) for easier Variable 
Development with Spark (and Scala).

The SmvApp framework with all the additional functions is to make user's experience closer to solve the data problem 
with minimal Spark/Scala programing knowledge and skills. 

## Application Framework
* [Application Framework Intro](docs/appFramework.md)
* [Application Configuration](docs/appConfig.md)

## Smv Functions
* [CSV Handling](docs/csvHandling.md)
* [Extended Data Dictionary](docs/Edd.md)
* [Column Helper functions](docs/ColumnFunctions.md)
* [DataFrame Helper functions](docs/DF_Functions.md)
* [DQM - Experimental](docs/Dqm.md)

## Start with the Example Project
[Example: 01GetStart](docs/examples/01GetStart) is an example project to help the 
user to quickly try oit SMV with some real data and also makes setting up their own project 
easy. 

## Migrate to Spark 1.3
Since Spark 1.3.0, there quite some interface changes on SparkSQL. Please refer 
[Migrate to Spark 1.3](docs/MigrateTo1.3.md) for details.


