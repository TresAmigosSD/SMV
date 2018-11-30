# SMV Application Configuration

SMV provides multiple ways to set the runtime configuration.  Multiple config files are utilized to
facilitate the ability to define static application level configuration and easily allow the user to
provide their own configuration.  This also allows the static application level configuration to be
"checked-in" with the individual apps.  In addition to the configuration files, the user is able
to override any of the options using the command line interface.

## Command line override

For any given option X, the user may override the value specified in any of the configuration files as follows:

```
smv-run --smv-props X=55 ... -m module_to_run
```

multiple properties may be specified at the same time:

```
smv-run --smv-props smv.appName="myApp" smv.stages="s1,s2" ... -m module_to_run
```

Some configuration parameters have a shorthand direct command line override (e.g. --data-dir)

## Name conflict resolution

If a configuration parameter appears in multiple places, then the conflict is resolved by picking the value from the higher priority source.
The sources are ordered from high to low priority as follows:

* command line options
* project user level config file (default: conf/smv-user-conf.props)
* global user level config file (~/.smv/smv-user-conf.props)
* app level config file (default: conf/smv-app-conf.props)
* smv default options

Since SMV support both `smv-user-conf.props` and `smv-app-conf.props`, a typical practice is to
put project level shared (accross team members) config into `smv-app-conf.props` and user
specific config into `smv-user-conf`. Also config the version control system to ignore
`conf/smv-user-conf.props`.

The property files `smv-app-conf.props` and `smv-user-conf.props` are standard Jave property files.
Here is an `smv-app-conf.props` example:
```
# application name
smv.appName = My Sample App

# stage definitions
smv.stages = org.tresamigos.sample.etl.internal, \
             org.tresamigos.sample.etl.external, \
             org.tresamigos.sample.dm.account, \
             org.tresamigos.sample.dm.customer

# spark sql properties
spark.sql.shuffle.partitions = 256

# Runtime config
smv.config.keys=sample
smv.config.sample=full
```

Here is an `smv-user-conf.props` example:
```
# data dir
smv.dataDir = hdfs:///user/myunixname/data
smv.inputDir = hdfs:///applications/project/data/landingzone

smv.config.sample=1pct
```

## SMV Config Parameters

The table below describes all available SMV configuration parameters.
Note that for sequence/list type parameters (e.g. smv.stages), a "," or ":" can be used to separate out the items in the list.
**For command line arguments, only the ":" can be used as a separator.**
<table>
<tr>
<th>Property Name</th>
<th>Default</th>
<th>Required/<br>Optional</th>
<th>Description</th>
</tr>

<tr>
<td>smv.appName</td>
<td>"SMV Application"</td>
<td>Optional</td>
<td>The application name</td>
</tr>

<tr>
<td>smv.stages</td>
<td>empty</td>
<td>Required</td>
<td>List of stage names in application.<br>Example: "etl, model, ui"</td>
</tr>

<tr>
<td>smv.forceEdd</td>
<td>empty</td>
<td>Optional</td>
<td>When set to "true" or "True", a run will force edd calculation on all modules, and persisted in metadata</td>
</tr>

<tr>
<td>smv.lock</td>
<td>False</td>
<td>Optional</td>
<td>When set to "true" or "True", data persisting and metadata persisting will be combined in an atom with file base lock. The lock files will be under <code>smv.lockDir</code></td>
</tr>

<tr>
<td>smv.sparkdf.defaultPersistFormat</td>
<td>parquet_on_hdfs</td>
<td>Optional</td>
<td>Specify default data persisting format for Spark DF data, either `smvcsv_on_hdfs` or `parquet_on_hdfs`.</td>
</tr>

<tr>
<th colspan="4">Data Directories Parameters</th>
</tr>

<tr>
<td>smv.dataDir</td>
<td>N/A</td>
<td>Required</td>
<td>The top level data directory.
Can be overridden using <code>--data-dir</code> command line option</td>
</tr>

<tr>
<td>smv.inputDir</td>
<td>dataDir<code>/input</code></td>
<td>Optional</td>
<td>Data input directory
Can be overridden using <code>--input-dir</code> command line option</td>
</tr>

<tr>
<td>smv.outputDir</td>
<td>dataDir<code>/output</code></td>
<td>Optional</td>
<td>Data output directory
Can be overridden using <code>--output-dir</code> command line option</td>
</tr>

<tr>
<td>smv.lockDir</td>
<td>dataDir<code>/lock</code></td>
<td>Optional</td>
<td>If <code>smv.lock</code> specified, dir for lock files</td>
</tr>

<tr>
<td>smv.publishDir</td>
<td>dataDir<code>/publish</code></td>
<td>Optional</td>
<td>Data publish directory
Can be overridden using <code>--publish-dir</code> command line option</td>
</tr>

<tr>
<th colspan="4">Jdbc Parameter</th>
</tr>

<tr>
<td>smv.jdbc.url</td>
<td>None</td>
<td>Required for use with JDBC, otherwise optional</td>
<td>JDBC url to use for publishing and reading tables</td>
</tr>

<tr>
<td>smv.jdbc.driver</td>
<td>None</td>
<td>Required for use with JDBC, otherwise optional</td>
<td>JDBC driver to use for publishing and reading tables</td>
</tr>

</table>

Please refer [Runtime User Configuration](run_config.md) for details and examples
of how to use runtime user specified configuration.

## Data Directories configuration
SMV will access either local or HDFS for CSV input, module persistency and [publish](smv_stages.md).
User have to specify `smv.dataDir` either through the configuration files or command line.

There are 3 other parameters for specify the detail location of input, persisted and published data:
* `smv.inputDir`
* `smv.outputDir`
* `smv.publishDir`

With only `smv.dataDir` specified, 3 sub-dir, `input`, `output`, `publish` will be used as the default
values.

Typically for a team setup, each member should have their own project code, and their own `outputDir`.

For example, all members share the same data input under `hdfs:///applications/project/data/landingzone`,
and share a published dir `hdfs:///applications/project/shareddata`. The `smv-user-conf.props` for a
team member will look like,

```
smv.dataDir = hdfs:///user/myunixname/data
smv.inputDir = hdfs:///applications/project/data/landingzone
smv.publishDir = hdfs:///applications/project/shareddata
```

In this case the persisted data will be under `hdfs:///user/myunixname/data/output`, and input/published
data will in their specified location.


## Spark SQL configuration parameters.

For small projects, user may also provide spark SQL configuration properties along with the SMV properties.
This reduces the number of configuration files to maintain and also allows the application to specify a better default
spark configuration parameter depending on its needs.  All properties starting with "spark.sql" will be added to the
runtime SqlContext configuration.
