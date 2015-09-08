# SMV Getting Started

## Create example App

SMV provides a shell script to easily create an example application.
The example app can be used for exploring SVM and it can also be used as an initialization script for a new project.

The `smv_init.sh` script can be used to create the initial SMV app.
smv_init.sh only requires two parameters.
* The name of the directory to create for the application
* The FQN of the package to use for Maven and source files.
For example:

```bash
$ _SMV_HOME_/tools/bin/smv_init.sh MyApp com.mycompany.myapp
```

The above command will create the ```MyApp``` directory and
install the source, configuration, and build files required for a minimal example SMV app.

The rest of this document assumes the above command was run to show what is generated and how to use it.

**Note:**  User can skip to the "Run Example App" section if they are not interested in exploring the output from `smv_init.sh`

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
The example app generates an app with a single stage `stage1` and two packages.
While a single package would have sufficed, it is best to separate out all the input files
of a stage into a separate package.
The input files and their DQM rules/policies are put in the `input` sub-package

The generated `etl` package contains a single sample module for processing the provided employment data.

## Build Example App
The generated application must be built before it is run.  This is simply done by running the following maven command:

```shell
$ mvn clean install
```

The above command should generate a target directory that contains the application "fat" jar `myapp-1.0-SNAPSHOT-jar-with-dependencies.jar`.
This jar file will contain the compiled application class files, all the SMV class files and everything else that SMV depends on (except for the Spark libraries)

## Run Example App
The built app can be run by two methods.
* `run_app.sh` : used to run specific modules, stages, or entire app from the command line.
* `run_shell.sh` : uses the Spark Shell to interactively run and explore the output of individual modules and files.

### Run Example App using `run_app.sh`
```shell
# run entire app (run all output modules in all stages)
$ _SMV_HOME_/tools/run_app.sh --run-app

# run stage1 (all output modules in stage1)
$ _SMV_HOME_/tools/run_app.sh -s stage1

# run specific module (any module can be run this way, does not have to be an output module)
$ _SMV_HOME_/tools/run_app.sh -m com.mycompany.myapp.stage1.etl.EmploymentRaw
```

See [SMV Output Modules](smv_module.md#output-modules) for more details on how to mark a module as an output module.

The output csv file and schema can be found in the `data/output` directory (as configured in the `conf/smv-user-conf.props` files).

```shell
$ cat data/output/com.mycompany.myapp.stage1.etl.EmploymentRaw_XXXXXXXX.csv/part-* | head -5
"32",981295
"33",508120
"34",3324188
"35",579916
"36",7279345

$ cat data/output/com.mycompany.myapp.stage1.etl.EmploymentRaw_XXXXXXXX.schema/part-*
FIRST('ST): String
EMP: Long
```

**Note**: the output above may be different as it depends on order of execution of partitions.

See [Run SMV Application](run_app.md) for further details.

TODO: add example of running with -g (graph) command.

### Run Example App using `run_shell.sh`

#### Launch shell
Spark shell can be used to allow the user to run individual modules interactively.
The `run_shell.sh` script is provided by SMV to make it easy to launch the Spark shell with the "fat" jar attached.

```shell
$ _SMV_HOME_/tools/run_shell.sh
```

TODO: add link to run_shell.sh reference doc!

#### Exploring SmvDataSets (SmvFiles and SmvModules)
Once we are inside the spark shell, we can "source" (using the `s()` smv shell helper function) any `SmvFile` or `SmvModule` instance
and inspect the contents (because the `s` function returns a standard Spark `DataFrame` object)

```scala
scala> val d1=s(employment)

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
scala> val d2 = s(EmploymentRaw)
d2: org.apache.spark.sql.DataFrame = [FIRST('ST): string, EMP: bigint]

scala> d2.printSchema
root
 |-- FIRST('ST): string (nullable = true)
 |-- EMP: long (nullable = true)


scala> d2.count
res2: Long = 52
```

`EmploymentRaw` is defined in the `etl` package `Employment.scala` file.
As you can see above, when you try to refer to a SmvModule, it will do the calculation and
then persist it for future use. Now you can use `d2`, a DataFrame, to refer to the
SmvModule output, although in this example, there are nothing intersecting in that
data other than the `EMP` field.

#### Run EDD on data
To quickly get an overall idea of the input data, we can use the SMV EDD (Extended Data Dictionary) tool.

```scala
scala> d1.select("ZIPCODE", "YEAR", "ESTAB", "EMP").edd.addBaseTasks().addHistogramTasks("ESTAB", "EMP")().dump
Total Record Count:                         38818
ZIPCODE              Non-Null Count:        38818
ZIPCODE              Average:               49750.09928383732
ZIPCODE              Standard Deviation:    27804.662445936523
ZIPCODE              Min:                   501
ZIPCODE              Max:                   99999
...
Histogram of ESTAB: with BIN size 100.0
key                      count      Pct    cumCount   cumPct
0.0                      26060   67.13%       26060   67.13%
100.0                     3129    8.06%       29189   75.19%
200.0                     1960    5.05%       31149   80.24%
300.0                     1445    3.72%       32594   83.97%
400.0                     1136    2.93%       33730   86.89%
500.0                      937    2.41%       34667   89.31%
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

TODO: add link to EDD ref guide here

