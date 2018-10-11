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
