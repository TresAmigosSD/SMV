# Runtime User Configuration (Python)

It is often necessary to modify the behavior of modules on a per run basis.  For example, users may utilize the full data in production but only run on sampled data during development.  SMV enables the user to define a set of key/value pairs that can be specified at run time.

## 1. Configuration
Users enumerate the app runtime config parameters in a config file.  This is usually the `conf/smv-app-conf.props` as the list of config parameters should not change.  Individual values can be overridden by user.  It would be good practice to also include valid default values for each of the user config parameters.

```
# conf/smv-app-conf.props
smv.config.keys=sample, filetype
smv.config.sample=full
smv.config.filetype=csv
```

## 2. Modules
Modules that need access to config values can access them via the `smvGetRunConfig` method. If the value is expected to be `bool` or `int`, the string value can be coerced with `smvGetRunConfigAsBool` or `smvGetRunConfigAsInt` respectively. Typically, if your module's behavior depends on a config value then its hash should change when the value does - i.e. its cached output should be invalidated. To achieve this, declare dependencies on config values with `requiresConfig`.

```python
class MyModule(SmvModule):
  def requiresConfig(self):
    return ["sample"]
  def run(self, i):
    ...
    if self.smvGetRunConfigAsBool("sample"):
      res = res.sample(0.01)
    return res
```

The above is available on all `SmvModules`. For backwards compatibility, SMV still supports `SmvRunConfig` and modules that mix in `SmvRunConfig` will continue to behave the same as before - the cache will be invalidated if the value for **any** key in `smv.config.keys` changes, even if they are not declared or used in your module. Note that by declaring `requiresConfig` instead of mixing in `SmvRunConfig`, cache invalidation can be limited to when relevant keys change.

## 3. Running
The user can change the current value of any config parameter on a per run basis.  This can be done in one of two ways:
* modify the `conf/smv-user-conf.props` or `~/.smv/smv-user-conf.props` to set the appropriate `smv.config.key=value` line.
* Override the property value from the command line.  For example, to set the sampling rate to 1pct for a run, add the following to the end of the `smv-run` command: `--smv-props smv.config.sample=1pct`

## 4. File Type example
It is often necessary to use Hive tables on the cluster and CSV files on development machines.  SMV run configuration can be used to facilitate this use case.  Both the Hive table and CSV file should be defined in the `inputdata.py` file.  Only the require method of the module needs to decide which to depend on at run time.  For example:
```python
# stage1/inputdata.py
class EmpCSV(SmvCsvFile):
    def path(self):
        return "input/employment/CB1200CZ11.csv"
class EmpHive(SmvHiveTable):
    def tableName(self):
        return "schema.tablename"
```

```python
import * from inputdata

class Employment(SmvModule, SmvRunConfig):
  def requireDS(self):
    if self.smvGetRunConfig("filetype") == "hive":
      return [ EmpHive ]
    else:
      return [ EmvCsv ]
```

# Runtime User Configuration (Scala)

In some situations, such as building subsets by filtering, different sets `DataFrame`s are genereated from the same input with almost the same _logic_.  Using a _configurable_ module enables code reuse while minizing boilerplate.

In addition to input modules specified with the `requireDS()` method, a _configurable_ module depends on a specific configuration object, that must be a subclass of the `SmvRunConfig` trait, to produce its desired output.  Which configuration object to use is specified by the value of `smv.runConfObj`, either in the application's <a href="app_config.md">configuration</a> file, or on the command line with `--run-conf-obj <name>` option or with `--smv-props smv.runConfObj=<name>`.

By default, the name of the configuration object is the fully qualified class name (FQN) of the implementing object.  However, one could override `runConfig` in trait `Using[T]` to change the way the configuration object is obtained.

Below is an example of how to use a configurable module:

```scala
trait BaseStudio extends SmvRunConfig {
  def actors: Seq[String]
  def directors: Seq[String]
}

object Hollywood extends BaseStudio {
  override val actors = Seq("Hillary Clinton", "Bernie Sanders")
  override val directors = Seq("George Soros")
}

object Bollywood extends BaseStudio {
  override val actors = Seq()
  override val directors = Seq()
}

object Budget extends SmvModule("Projects cost of film production") with Using[BaseStudio] {
  ...
  override def run (i: runParams) = {
    // Access to the actual configuration is provided by the runConfig object
    val available = df.where(df("name").isin(runConfig.actors))
    ....
  }
}
```

Here the run configuration is defined with the `BaseStudio` trait, which extends `SmvRunConfig`.  It contains two pieces of information: lists of available actors and directors.  There are two specific configurations specified by `Hollywood` and `Bollywood`, each with its own list of available actors and directors.  And the module `Budget` declares that it needs the information from a `BaseStudio` by mixing in the trait `Using[BaseStudio]`, which provides the actual configuration through the `runConfig` object.  To specify the use of `Hollywood`, one invokes `smv-run --smv-props runConfObj=Hollywood`; to use `Bollywood`, one invokes `smv-run --run-conf-obj Bollywood`.  Both ways of specifying a configuration object on the commandline work with smv-run.  However, because smv-shell does not directly run the main program in SmvApp -- and therefore will not pass any commandline arguments to SmvApp -- the only way to specify a `runConfig` object, when you are running an interactive shell, is through the use of an SMV configuration file.

`SmvRunConfig` can contain arbitrary information, including `SmvDataSet`s.  For example, the first stage of processing often involves concatenating data sets from different sources, with some preliminary processing such as null-sanitization.  The data source may be CSV files or Hive tables.  The following code shows how to use `SmvRunConfig` in this situation:

```scala
trait BaseAppInput extends SmvRunConfig {
  def in_01: SmvDataSet
  def in_02: SmvDataSet
}

object CsvAppInput extends BaseAppInput {
  override val in_01 = SmvCsvFile("some/file.csv")
  override val in_02 = SmvCsvFile("some/other/file.csv")
}

object HiveAppInput extends BaseAppInput {
  override val in_01 = SmvHiveTable("some_table")
  override val in_02 = SmvHiveTable("some_other_table")
}

object ConcatInput extends SmvModule("Concatenate input data sets") with Using[BaseAppInput] {
  override def reuqiresDS = Seq(runConfig.in_01, runConfig.in_02)
  override def run (i: runParams) = {
    // load the first input data set, specified with a runtime configuration
    val df1 = i(runConfig.in_01)
    ...
  }
}
```
