# Running SMV Application

While an SMV application can be built and run using the standard "spark-submit" command,
a convenient script `smv-run` is provided to make it easier to make running an application. No build is necessary if project is purely in Python.

### Synopsis
```shell
$ smv-run [smv-options] [what-to-run] -- [standard spark-submit-options]
```

**Note:**  The above command should be run from the project top level directory.

### Options
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
<td>--script</td>
<td>None</td>
<td>allow user to specify a script to launch instead of default `runapp.py`
<br>
<code>$ ... --script myapp.py</code>
</td>
</tr>

<tr>
<td>--smv-props</td>
<td>None</td>
<td>allow user to specify a set of config properties on the command line.
<br>
<code>$ ... --smv-props "smv.stages=s1"</code>
<br>
See <a href="app_config.md">Application Configuration</a> for details.
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
<code>$ dot -Tpng MyApp.dot -o graph.png</code>
</td>
</tr>

<tr>
<td>--force-run-all</td>
<td>off</td>
<td>Remove <b>ALL</b> files in output directory that <b>are</b> the  current version of the outputs in the app, forcing all specified modules to run even if they have been run recently.
</tr>

<tr>
<td>--publish version</td>
<td>off</td>
<td>publish the specified modules to the given version</td>
</tr>

<tr>
<td>--publish-hive</td>
<td>off</td>
<td>publish the specified modules to specified Hive tables</td>
</tr>

<tr>
<td>--publish-jdbc</td>
<td>off</td>
<td>
publish the specified modules through JDBC
**NOTE:** You must specify the JDBC url to use via the config property `smv.jdbc.url`
</td>
</tr>

<tr>
<td>--export-csv (previously --publish-local)</td>
<td>None</td>
<td>
publish the specified modules to the local file system
</td>
</tr>

<tr>
<td>--dry-run </td>
<td>off</td>
<td>Find which modules do not have persisted data, among the modules that need to be run. When specified, no modules are actually executed.
</td>
</tr>

<tr>
<td>--dead</td>
<td>off</td>
<td>Print a list of the dead modules in this application
</td>
</tr>

<tr>
<td>--spark-home</td>
<td>location of spark-submit</td>
<td>Location where SMV should find Spark installation.
</td>
</tr>

<tr>
<td>--script</td>
<td>run driver script instead of runapp.py</td>
<td>Location of driver script.
</td>
</tr>

<tr>
<th colspan="3">What To Run/Publish
<br>
One of the options below must be specified.
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
<td>Run all output modules in given stages.  The stage name can either be the base name of the stage or the FQN.
</td>
</tr>

<tr>
<td colspan="2">--run-app, -r </td>
<td>Run all output modules in all configured stages in current app.
</td>
</tr>

<tr>
<th colspan="3">Data Directories Config Override</th>
</tr>

<tr>
<th>Option</th>
<th>Config<br>Override</th>
<th>Description</th>
</tr>

<tr>
<td>&#8209;&#8209;data&#8209;dir</td>
<td>smv.dataDir</td>
<td>option to override default location of top level data directory</td>
</tr>

<tr>
<td>&#8209;&#8209;input&#8209;dir</td>
<td>smv.inputDir</td>
<td>option to override default location of input data directory</td>
</tr>

<tr>
<td>&#8209;&#8209;output&#8209;dir</td>
<td>smv.outputDir</td>
<td>option to override default location of output data directory</td>
</tr>

<tr>
<td>&#8209;&#8209;publish&#8209;dir</td>
<td>smv.publishDir</td>
<td>option to override default location of publish directory</td>
</tr>

</table>

### Spark commands override
Users are able to override the name of the spark commands used to launch batch jobs and shell.  The two environment variables `SMV_SPARK_SUBMIT_CMD` and `SMV_PYSPARK_CMD` can be used to override the `spark-submit` and `pyspark` commands respectively.
For example, on a cloudera system with both spark 1.6 and spark 2.2, user can set the following in their shell profile:
```bash
# user's .bash_profile
export SMV_SPARK_SUBMIT_CMD="spark2-submit"
export SMV_PYSPARK_CMD="pyspark2"
```

The above would ensure that SMV users the spark 2.2 version on the system.

### Examples
Run modules `M1` and `M2` and all its dependencies.  Note the use of the module FQN.
```shell
$ smv-run -m com.mycom.myproj.stage1.M1 com.mycom.myproj.stage1.M2
```

Run modules `M1` and `M2` and all its dependencies. Publish to the Hive tables as specified
by the `tableName` method of `M1` and `M2`.
```shell
$ smv-run --publish-hive -m com.mycom.myproj.stage1.M1 com.mycom.myproj.stage1.M2
```

Run all modules in application and generate edd report for all modules that needed to run (including dependencies)
```shell
$ smv-run --edd --run-app
```

Generate a dependency graph for all modules that needed to run.
```shell
$ smv-run -g --run-app
```

Publish the output modules in stage "s1" as version "xyz".  The modules will be output to `/tmp/publish/xyz` dir.
```shell
$ smv-run --publish xyz --publish-dir /tmp/publish -s s1
```

Provide spark specific arguments
```shell
$ smv-run -m com.mycom.myproj.stage1.M1 -- --executor-memory=2G --driver-memory=1G --master yarn-client
```
