# Runtime User Configuration

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
Modules that need to access a configuration parameter must inherit from the mix-in class `SmvRunConfig`.  The `SmvRunConfig` class will provide the `smvGetRunConfig` method to access the user runtime config by key.  Additional helper methods are provided by `SmvRunConfig` to retrieved "typed" values (e.g. `smvGetRunConfigAsBool` and `smvGetRunConfigAsInt`)

```python
class MyModule(SmvModule, SmvRunConfig):
  def run():
    if smvGetRunConfig("sample") == "1pct":
      df = df.sample(0.01)
    return df
```

The module hash value of modules that inherit from `SmvRunConfig` will be affected by the current config values.  The hash will utilize **ALL** the config values even ones that are not used by the current module.

## 3. Running
The user can change the current value of any config parameter on a per run basis.  This can be done in one of two ways:
* modify the `conf/smv-user-conf.props` or `~/.smv/smv-user-conf.props` to set the appropriate `smv.config.key=value` line.
* Override the property value from the command line.  For example, to set the sampling rate to 1pct for a run, add the following to the end of the `smv-pyrun` command: `--smv-props smv.config.sample=1pct`

## 4. File Type example
It is often necessary to use Hive tables on the cluster and CSV files on development machines.  SMV run configuration can be used to facilitate this use case.  Both the Hive table and CSV file should be defined in the `inputdata.py` file.  Only the require method of the module needs to decide which to depend on at run time.  For example:
```python
# stage1/inputdata.py
class EmpCSV(SmvPyCsvFile):
    def path(self):
        return "input/employment/CB1200CZ11.csv"
class EmpHive(SmvPyHiveTable):
    def tableName(self):
        return "schema.tablename"
```

```python
import * from inputdata

class Employment(SmvPyModule, SmvRunConfig):
  def requireDS(self):
    if smvGetRunConfig("filetype") == "hive":
      return [ EmpHive ]
    else:
      return [ EmvCsv ]
```
