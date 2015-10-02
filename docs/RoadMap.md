# Dynamically show dependency graph

Current driver can generated dot plot by given a target module. Will support a more interactive
tool to browse the entire dependency graph.

When zoom out, the graph units are stages, when zoom
in each stage should show input/output modules and the follow and dependency within the stage.
Zoom in on a specific module, user show see the module schema and meta data.

# Using R Shell to create / use SMV modules.

Now that RShell is an integral part of Spark Core, we would like to utilize R Shell to not only use predefined modules but also create them.

## Using existing SmvModules
At a minimum, users should be able to "run" SMV modules defined in scala from R Shell.
This will require that the user define the module in scala and then refresh the R Shell to load the new Smv Module and run it.
"Running" an SMV module means converting the output of the module into a local R data frame.

## Creating new SMV modules from R.
Users can always create helper function in R that wrap around a sequence of operations that manipulate SMV modules to form
more complex objects.  Essentially, replicate the SMV module concept in R.
However, the above would make it impossible to integrate the above functions into the normal SMV stage flow.
Defining the module in Scala and then running in R does solve the above problem, but it requires the user to use Scala.

In the ideal solution, users should be able to define a proper SMV Module in R.
The user will call a function to start a new module (perhaps specifying the name, package, stage, etc) and the
dependencies (input datasets).
The user should then use the current Spark R library to transform the inputs as needed (except that they are now wrappers around datasets instead of Scala DataFrames).
Once the set of transformations has been completed, the user can then "publish" the data set.
This will create a corresponding Scala SMV module.
The new SMV module code will reside in the appropriate package/stage and is accessible by other Scala SMV modules **AND** from R Shell.

**Note**: the above technique can also be used for PySpark to enable users to define SMV modules in python.
