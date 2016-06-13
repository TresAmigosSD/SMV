# SmvModule

An SMV Module is a collection of transformation operations and validation rules.  Each module depends on one or more `SmvDataSet`s (`SmvFile` or `SmvModule`) and defines a set of transformation on its inputs that define the module output.

## Module Dependency Definition
Each module **must** define its input dependency by overriding the `requireDS` method. The `requireDS` method should return a sequence of required datasets for the running of this module.
The dependent datasets **must** be defined in the same stage as this module.  External dependencies should use the `SmvModuleLink` class as described in [SmvStage](smv_stages.md).

```scala
object MyModule extends SmvModule("mod description") {
 override def requireDS() = Seq(Mod1, Mod2)
 ...
```

Note that `requireDS` returns a sequence of the actual `SmvDataSet` objects that the module depends on, **not** the name.  This makes typos visible at compile time rather than run time.
The dependency can be on any combination of files/modules.  It is not limited to other `SmvModules`.
```scala
object MyModule extends SmvModule("mod description") {
 override def requireDS() = Seq(File1, File2, Mod1)
 ...
```
`File1`, `File2` are instances of `SmvFile` and `Mod1` is an instance of `SmvModule`.  From the perspective of `MyModule`, the type of the dependent dataset is irrelevant.

## Module Transformation Definition (run)
The module **must** also provide a `run()` method that performs the transformations on the inputs to produce the module output.  The `run` method will be provided with the results (DataFrame) of running the dependent input modules as a map keyed by the dependent module.

```scala
object MyModule extends SmvModule("mod description") {
 override def requireDS() = Seq(Mod1, Mod2)

 override def run(inputs: runParams) = {
   val M1df = inputs(Mod1) // DataFrame result of running Mod1 (done by framework automatically)
   val M2df = inputs(Mod2)

   M1df.join(M2df, ...)
       .select("col1", "col2", ...)
 }

```

The `run` method should return the result of the transformations on the input as a `DataFrame`.

The parameter of the `run` method has type `runParams`, which is just an alias to type
`Map[SmvDataSet, DataFrame]`. The driver program (Smv framework itself) will provide
the lookup map, "inputs: runParams", to map all the required modules to their
output `DataFrame`. As in above example, `val M1df = inputs(Mod1)` provides the `run`
method the access to the result `DataFrame` of module `Mod1`.

## Module Validation Rules
Each module may also define its own set of [DQM validation rules](dqm.md).  By default, if the user does not override the `dqm` method, the module will have an empty set of rules.

## Module Persistence
To aid in development and debugging, the output of each module is persisted by default.  Subsequent requests for the module output will result in reading the persisted state rather than in re-running the module.
The persisted file is versioned.  The version is computed from the CRC of this module and all dependent modules.  Therefore, if this module code or any of the dependent module code changes, then the module will be re-run.
On a large development team, this makes it very easy to "pull" the latest code changes and regenerate the output by only running the modules that changed.

However, for trivial modules (e.g. filter), it might be too expensive to persist/read the module output.  In these cases, the module may override the default persist behaviour by setting the `isEphemeral` flag to true.  In that case, the module output will not be persisted (unless the module was run explicitly).

```scala
object MyModule extends SmvModule("mod description") {
  override val isEphemeral = true
  override def run(inputs: runParams) = {
     inputs(Mod1).where($"a" > 100)
  }
}
```

## Configurable Modules
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

Here the run configuration is defined with the `BaseStudio` trait, which extends `SmvRunConfig`.  It contains two pieces of information: lists of available actors and directors.  There are two specific configurations specified by `Hollywood` and `Bollywood`, each with its own list of available actors and directors.  And the module `Budget` declares that it needs the information from a `BaseStudio` by mixing in the trait `Using[BaseStudio]`, which provides the actual configuration through the `runConfig` object.  To specify the use of `Hollywood`, one invokes `smv-run --smv-props runConfObj=Hollywood`; to use `Bollywood`, one invokes `smv-shell --run-conf-obj Bollywood`.  Both ways of specifying a configuration object work with smv-shell or smv-run.

# Output Modules
As the number of modules in a given SMV stage grows, it becomes more difficult to track which
modules are the "leaf"/output modules within the stage.
Any module or SmvDataSet within the stage can be marked as an output module by mixing-in the `SmvOutput` trait.
For example:

```scala
object MyModule extends SmvModule("this is my module") with SmvOutput {
...
}
```

```scala
object MyData extends SmvCsvFile("path/to/file/data.csv", CA.ca) with SmvOutput
```

The set of `SmvOutput` output modules in a stage define the data *interface/api* of the stage.  Since modules outside this stage can only access modules marked as output, non-output modules can be changed at will without any fear of affecting external modules.

In addition to the above, the ability to mark certain modules as output has the following benefits:

* Allows user to easily "run" all output modules within a stage (using the `-s` option to `smv-run`)
* A future option might be added to allow for listing of "dead" modules.  That is, any module in a stage that does not contribute to any output module either directly or indirectly.
* We may add a future option to `SmvApp` that allows the user to display a "catalog" of output modules and their description.

See [Smv Stages](smv_stages.md) for details on how to configure multiple stages.
