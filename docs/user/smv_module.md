# SmvModule

# Output Modules
As the number of modules in a given SMV stage grows, it becomes more difficult to track which
modules are the "leaf"/output modules within the stage.
Any module within the stage can be marked as an output module by mixing-in the `SmvOutput` trait.
For example:

```scala
object MyModule extends SmvModule("this is my module") with SmvOutput {
...
}
```

The set of `SmvOutput` output modules in a stage define the *interface/api* of the stage.
Since modules outside this stage can only access modules marked as output,
none output modules can be changed at will without any fear of affecting external modules.

In addition to the above, the ability to mark certain modules as output has the following benifits:

* Allows user to easily "run" all output modules within a stage (using the `-s` option to `smv-run`)
* A future option might be added to allow for listing of "dead" modules.  That is, any module in a stage that does not contribute to any output module either directly or indirectly.
* We may add a future option to `SmvApp` that allows the user to display a "catalog" of output modules and their description.

See [Smv Stages](smv_stages.md) for details on how to configure multiple stages.

TODO: renmae this to smv_modules.md and add info about modules in general (not just output) with examples!
TODO: add explanation about requireDS, run, etc for a normal module.  dependency on SmvFile vs SmvModule, ...
