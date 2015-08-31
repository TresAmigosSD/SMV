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

See [Application Configuration](docs/ref/app_config.md) for more details about availabe configuration parameters.

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

## Run Example App

TODO: add run_app and run_shell here!!!!