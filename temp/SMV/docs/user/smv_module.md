# SmvModule

An SMV Module is a collection of transformation operations and validation rules.  Each module depends on one or more `SmvDataSet`s and defines a set of transformation on its inputs that define the module output.

## Module Dependency Definition
Each module **must** define its input dependency by overriding the `requiresDS` method. The `requiresDS` method should return a sequence of `SmvDataSet`s required as input for this module.
The dependent datasets can be defined in the same or a different stage as this module (see [SmvStage](smv_stages.md) for details).

```Python
class MyModule(smv.SmvModule):
  def requiresDS(self):
    return [Mod1,Mod2]
```

Note that `requiresDS` returns a sequence of the actual `SmvDataSet` objects that the module depends on, **not** the name. The dependency can be on any combination of `SmvDataSet`s which may be files, Hive tables, modules, etc. It is not limited to other `SmvModules`.

```python
class MyModule(smv.SmvModule):
  def requiresDS(self):
    return [File1,Hive2,Mod3]
```

## Module Transformation Definition (run)
The module **must** also provide a `run()` method that performs the transformations on the inputs to produce the module output.  The `run` method will be provided with the results (DataFrame) of running the dependent input modules as a map keyed by the dependent module.

```Python
class MyModule(smv.SmvModule):
  def requiresDS(self):
    return [Mod1,Mod2]
  def run(self, i):
    m1df = i[Mod1]
    m2df = i[Mod2]
    return M1df.join(M2df, ...).select("col1", "col2", ...)
```

The `run` method should return the result of the transformations on the input as a `DataFrame`.

The parameter `i` of the `run` method maps `SmvDataSet` to its resulting `DataFrame`. The driver (Smv Framework) will run the dependencies of the `SmvDataSet` to provide this map.

## Module Validation Rules
Each module may also define its own set of [DQM validation rules](dqm.md).  By default, if the user does not override the `dqm` method, the module will have an empty set of rules.

## Module Persistence
To aid in development and debugging, the output of each module is persisted by default.  Subsequent requests for the module output will result in reading the persisted state rather than in re-running the module.
The persisted file is versioned.  The version is computed from the CRC of this module and all dependent modules.  Therefore, if this module code or any of the dependent module code changes, then the module will be re-run.
On a large development team, this makes it very easy to "pull" the latest code changes and regenerate the output by only running the modules that changed.

However, for trivial modules (e.g. filter), it might be too expensive to persist/read the module output.  In these cases, the module may override the default persist behaviour by setting the `isEphemeral` flag to true.  In that case, the module output will not be persisted (unless the module was run explicitly).

### Python
```python
class MyModule(smv.SmvModule):
  def isEphemeral(self): return False
  ....    
```

# Output Modules
As the number of modules in a given SMV stage grows, it becomes more difficult to track which modules are the "leaf"/output modules within the stage. Any module or `SmvDataSet` within the stage can be marked as an output module by mixing-in the `SmvOutput` trait.

The set of `SmvOutput` output modules in a stage define the data *interface/api* of the stage. The ability to mark certain modules as output has the following benefits:

* Allows user to easily "run" all output modules within a stage (using the `-s` option to `smv-run`). Depending on the options specified, they can then be published to CSV or to Hive.
* A future option might be added to allow for listing of "dead" modules.  That is, any module in a stage that does not contribute to any output module either directly or indirectly.
* We may add a future option to `SmvApp` that allows the user to display a "catalog" of output modules and their description.

See [Smv Stages](smv_stages.md) for details on how to configure multiple stages.

## Simple Publish To Hive
If you would like to publish the module to a Hive table, include a `tableName`, and use `--publish-hive` command line parameter to publish/export the output to the specified Hive table (For advanced use case, see section below).  For example:

```python
class MyModule(smv.SmvModule, smv.SmvOutput):
  def tableName(self):
    return "hiveschema.hivetable"
  ...
class MyFile(smv.SmvCsvFile, smv.SmvOutput):
  ...
```

## Advanced Publish To Hive
A more advanced alternative to the `tableName` method described above is to provide a `publishHiveSql` method that returns a sql statement to do the actual publishing.  This could be any valid HQL query such as `insert overwrite` or `create table from...`.  The result of running the module is provided as a temporary table named `dftable`.

```python
class MyModule(smv.SmvModule, smv.SmvOutput):
  def publishHiveSql(self):
    return "insert overwrite table foo ... select * from dftable"
  ...
```

**Note:** it is possible to specify multiple statements in `publishHiveSql`.  Just need to separate them by `';'`.  For example:

```python
# python example:
class MyModule(smv.SmvModule, smv.SmvOutput):
  def publishHiveSql(self):
    return """
      drop table if exists bar;
      create table bar as select * from dftable
    """
  ...
```
