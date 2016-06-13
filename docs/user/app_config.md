# SMV Application Configuration

SMV provides multiple ways to set the runtime configuration.  Multiple config files are utilized to
facilitate the ability to define static application level configuration and easily allow the user to
provide their own configuration.  This also allows the static application level configuration to be
"checked-in" with the individual apps.  In addition to the configuration files, the user is able
to override any of the options using the command line interface.

## Command line override

For any given option X, the user may override the value specified in any of the configuration files as follows:

```
_SMV_HOME_/tools/smv-run --smv-props X=55 ... -m module_to_run
```

multiple properties may be specified at the same time:

```
_SMV_HOME_/tools/smv-run --smv-props smv.appName="myApp" smv.stages="s1,s2" ... -m module_to_run
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
<td>smv.stages.X.packages</td>
<td>empty</td>
<td>Required</td>
<td>List of package names in stage X.  The package names must be the fully qualified name.<br>
Example: "com.company.proj.etl, com.company.proj.model"</td>
</tr>

<tr>
<td>smv.stages.X.version</td>
<td>0</td>
<td>Optional</td>
<td>Current version of stage X. See <a href="smv_stages.md"> for details on stage versions</td>
</tr>

<tr>
<td>smv.runConfigObj</td>
<td>empty</td>
<td>Optional</td>
<td>The configuration object to use. See <a href="smv_module.md">SmvModule</a> for details</td>
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
<td>Data input directory (not currently used)
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
<td>smv.publishDir</td>
<td>dataDir<code>/publish</code></td>
<td>Optional</td>
<td>Data publish directory
Can be overridden using <code>--publish-dir</code> command line option</td>
</tr>

<tr>
<th colspan="4">Dynamic Class Server Parameters</th>
</tr>

<tr>
<td>smv.class_server.host</td>
<td>N/A</td>
<td>Optional</td>
<td>host name of class server.  If this is not specified, then the dynamic class server will not be utilized and SMV will look for modules in the normal CLASSPATH (using `Class.forName()` method).</td>
</tr>

<tr>
<td>smv.class_server.port</td>
<td>9900</td>
<td>Optional</td>
<td>port number where class server will be listening for connections and where SmvApp will connect to.</td>
</tr>

<tr>
<td>smv.class_server.class_dir</td>
<td>./target/classes</td>
<td>Optional</td>
<td>directory where the class server would look for new class instances.</td>
</tr>
</table>

## Spark SQL configuration parameters.

For small projects, user may also provide spark SQL configuration properties along with the SMV properties.
This reduces the number of configuration files to maintain and also allows the application to specify a better default
spark configuration parameter depending on its needs.  All properties starting with "spark.sql" will be added to the
runtime SqlContext configuration.
