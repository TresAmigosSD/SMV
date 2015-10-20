# SMV Getting Started

## Create example App

SMV provides a shell script to easily create an example application.
The example app can be used for exploring SVM and it can also be used as an initialization script for a new project.

The `smv-init` script can be used to create the initial SMV app. `smv-init` only requires two parameters.
* The name of the directory to create for the application
* The FQN of the package to use for Maven and source files.
For example:

```bash
$ _SMV_HOME_/tools/smv-init MyApp com.mycompany.myapp
```

The above command will create the ```MyApp``` directory and
install the source, configuration, and build files required for a minimal example SMV app.

The rest of this document assumes the above command was run to show what is generated and how to use it.

**Note:**  User can skip to the "Run Example App" section if they are not interested in exploring the output from `smv-init`

### Example App Configuration Files

The generated example app contains two configuration files.

* `smv-app-conf.props` : The application level configuration parameters.  This file should define the application name and the configured stages.
* `smv-user-conf.props` : The user level configuration parameters.  This file is normally **NOT** checked in

See [Application Configuration](app_config.md) for more details about available configuration parameters.

### Example App data directory

#### Example data source
The data directory contains a sample file extracted from US employment data.

```shell
$ wget http://www2.census.gov/econ2012/CB/sector00/CB1200CZ11.zip
$ unzip CB1200CZ11.zip
$ mv CB1200CZ11.dat CB1200CZ11.csv
```

More info can be found on [US Census site](http://factfinder.census.gov/faces/tableservices/jsf/pages/productview.xhtml?pid=BP_2012_00CZ1&prodType=table)

### Example App pom.xml
A valid pom.xml file is generated using the package name provided on the command line.
The last part of the provided package name is used as the Maven "artifactID" and the first section is used as the "groupID"
In the example above, the artifactId will be set to `myapp` and the groupID will be `com.mycompany`

### Example App sources
The example app generates an app with a two stages `stage1` and `stage2`.  Having two stages for such a tiny example is overkill, but they are there for demonstration purposes.
The generated source files are:
* `stage1/InputSetS1.scala` : contains definitions of all input files into stage1 and their DQM rules/policies.
* `stage1/EmploymentByState.scala` : contains sample ETL module for processing the provided employment data.
* `stage2/InputFilesS2.scala` : defines the input links to the output modules in `stage1`
* `stage2/StageEmpCategory.scala` : sample "modeling" module for creating categorical variables.

**Note:** In practice, a single stage will have multiple module files and possibly additional input files (depending on the number and complexity of inputs)

## Build Example App
The generated application must be built before it is run.  This is simply done by running the following maven command:

```shell
$ mvn clean install
```

The above command should generate a target directory that contains the application "fat" jar `myapp-1.0-SNAPSHOT-jar-with-dependencies.jar`.
This jar file will contain the compiled application class files, all the SMV class files and everything else that SMV depends on (except for the Spark libraries)

## Run Example App
The built app can be run by two methods.
* `smv-run` : used to run specific modules, stages, or entire app from the command line.
* `smv-shell` : uses the Spark Shell to interactively run and explore the output of individual modules and files.

### Run Example App using `smv-run`
```shell
# run entire app (run all output modules in all stages)
$ _SMV_HOME_/tools/smv-run --run-app

# run stage1 (all output modules in stage1)
$ _SMV_HOME_/tools/smv-run -s stage1
# or
$ _SMV_HOME_/tools/smv-run -s com.mycompany.myapp.stage1


# run specific module (any module can be run this way, does not have to be an output module)
$ _SMV_HOME_/tools/smv-run -m com.mycompany.myapp.stage1.EmploymentByState
```

See [SMV Output Modules](smv_module.md#output-modules) for more details on how to mark a module as an output module.

The output csv file and schema can be found in the `data/output` directory (as configured in the `conf/smv-user-conf.props` files).

```shell
$ cat data/output/com.mycompany.myapp.stage1.EmploymentByState_XXXXXXXX.csv/part-* | head -5
"32",981295
"33",508120
"34",3324188
"35",579916
"36",7279345

$ cat data/output/com.mycompany.myapp.stage1.EmploymentByState_XXXXXXXX.schema/part-*
FIRST('ST): String
EMP: Long
```

**Note**: the output above may be different as it depends on order of execution of partitions.

```scala
$ _SMV_HOME_/tools/smv-run -m com.mycompany.myapp.stage1.EmploymentByState -g
```

With the `-g` flag, instead of produce and persist the module, the module dependency
graph will be created as a `dot` file. It can be converted to `png` using the
`dot` command.

```shell
$ dot -Tpng com.mycompany.MyApp.stage1.EmploymentByState.dot -o graph.png
```
You main need to install `graphviz` on your system to use the `dot` command.

See [Run SMV Application](run_app.md) for further details.

### Run Example App using `smv-shell`

#### Launch shell
Spark shell can be used to allow the user to run individual modules interactively.
The `smv-shell` script is provided by SMV to make it easy to launch the Spark shell with the "fat" jar attached.

```shell
$ _SMV_HOME_/tools/smv-shell
```

See [Run Spark Shell](run_shell.md) for details.

#### Exploring SmvDataSets (SmvFiles and SmvModules)
Once we are inside the spark shell, we can "source" (using the `s()` smv shell helper function) any `SmvFile` or `SmvModule` instance
and inspect the contents (because the `s` function returns a standard Spark `DataFrame` object)

```scala
scala> val d1=s(stage1.input.employment)

scala> d1.count
res1: Long = 38818

scala> d1.printSchema
root
 |-- ST: string (nullable = true)
 |-- ZIPCODE: string (nullable = true)
...

scala> d1.select("ZIPCODE", "YEAR", "ESTAB", "EMP").show(10)
ZIPCODE YEAR ESTAB EMP
35004   2012 167   2574
35005   2012 88    665
...
```

You can also access SmvModules defined in the code.
This is **not** limited to output modules.

```scala
scala> val d2 = s(stage1.EmploymentByState)
d2: org.apache.spark.sql.DataFrame = [ST: string, EMP: bigint]

scala> d2.printSchema
root
 |-- ST: string (nullable = true)
 |-- EMP: long (nullable = true)

scala> d2.count
res2: Long = 52
```

`EmploymentByState` is defined in stage1 `EmploymentByState.scala` file.
As you can see above, when you try to refer to a SmvModule, it will do the calculation and
then persist it for future use. Now you can use `d2`, a DataFrame, to refer to the
SmvModule output, although in this example, there are nothing intersecting in that
data other than the `EMP` field.

#### Run EDD on data
To quickly get an overall idea of the input data, we can use the SMV EDD (Extended Data Dictionary) tool.

```scala
scala> d1.select("ZIPCODE", "YEAR", "ESTAB", "EMP").edd.summary().eddShow
ZIPCODE              Non-Null Count         38818
ZIPCODE              Min Length             5
ZIPCODE              Max Length             5
ZIPCODE              Approx Distinct Count  38989
YEAR                 Non-Null Count         38818
YEAR                 Min Length             4
YEAR                 Max Length             4
YEAR                 Approx Distinct Count  1
ESTAB                Non-Null Count         38818
ESTAB                Average                191.45262507084342
ESTAB                Standard Deviation     371.37743343837866
ESTAB                Min                    1.0
ESTAB                Max                    16465.0
EMP                  Non-Null Count         38818
EMP                  Average                2907.469241073729
EMP                  Standard Deviation     15393.485966796263
EMP                  Min                    0.0
EMP                  Max                    2733406.0

scala> d1.edd.histogram("ESTAB", "EMP").eddShow
Histogram of ESTAB: with BIN size 100.0
key                      count      Pct    cumCount   cumPct
0.0                      26060   67.13%       26060   67.13%
100.0                     3129    8.06%       29189   75.19%
200.0                     1960    5.05%       31149   80.24%
...
-------------------------------------------------
Histogram of EMP: with BIN size 100.0
key                      count      Pct    cumCount   cumPct
0.0                      15792   40.68%       15792   40.68%
100.0                     3132    8.07%       18924   48.75%
200.0                     1844    4.75%       20768   53.50%
300.0                     1235    3.18%       22003   56.68%
400.0                      988    2.55%       22991   59.23%
500.0                      738    1.90%       23729   61.13%
...
```

Please see [EDD doc](edd.md) for more details.
