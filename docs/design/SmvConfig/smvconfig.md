# SmvConfig (python)

Need to come up with a python equivalent of the scala SMV configuration.  However, the scala version has proven difficult to integrate multiple configs.  We must create a base class that implements all config interfaces.  This can become cumbersome when using multiple orthogonal configs.

For the python interface, the configuration is simple a set of key/value pairs.  It is up to the user to determine how these values are used.  They can be used to determine which files to input or as a simple constant value or a conditional for sampling.

The proposed interface is as follows:

## 1. Configuration
Users enumerate the app config parameters in a config file.  This is usually the `conf/smv-app-conf.props` as the list of config parameters should not change.  Individual values can be overridden by user.  It would be good practice to also include valid default values for each of the user config parameters.

```
# conf/smv-app-conf.props
smv.config.keys=key1, key2
smv.config.key1=true
smv.config.key2=blah
```

## 2. Modules
Modules that need to access a configuration parameter must inherit from or mix-in the `SmvConfig` class.  The `SmvConfig` class will provide the `getConfig` method to access the user config by key.  Additional helper methods can be added `SmvConfig` to retrieved "typed" values (e.g. `getConfigAsBool`, or `getConfigAsInt`, etc).  TBD how to do this inside of UDFs.

```python
class MyModule(SmvModule, SmvConfig):
  def run():
    if getConfigAsBool("key1"):
      df = df.selectPlus(...)
    return df
```

The module hash value of modules that inherit from `SmvConfig` will be affected by the current config values.  The hash will utilize **ALL** the config values even ones that are not used by the current module.  This is because we don't know which values the user needs. The alternative is to force the user to declare which values are being used in a given module.  Need to let this soak for a while before we decide.

## 3. Running
The user should be able to easily change the current value of any config parameter on a per run basis.  This can be done in one of two ways:
* modify the `conf/smv-user-conf.props` or `~/.smv/smv-user-conf.props`.
* provide one or more `--user-conf`/`-uc` command line flag to change the value.  For example: `$ smv-pyrun --user-conf key1=false --run-app`

# Code changes
* Create `SmvConfig` python class with a `getConfig` method.
* Add a `getUserConfig` to the SMV app python proxy.
* Add config parsing of `smv.config.keys` and the `smv.config.*` parameters.
* Add a `getUserConfig` to the scala configuration class.
