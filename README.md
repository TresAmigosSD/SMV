# Spark Modularized View (SMV)
Spark Modularized View enables users to build enterprise scale applications on Apache Spark platform.
Please refer to [User Guide](docs/user/0_user_toc.md) and
[API docs](http://tresamigossd.github.io/SMV/scaladocs/index.html#org.tresamigos.smv.package) for details.

**Note:** The sections below were extracted from the User Guide and it should be consulted for more detailed instructions.

# Install SMV

```shell
$ git clone https://github.com/TresAmigosSD/SMV.git
$ mvn clean install
```

# SMV Getting Started

## Create example App

SMV provides a shell script to easily create an example application.
The example app can be used for exploring SVM and it can also be used as an initialization script for a new project.

```bash
$ _SMV_HOME_/tools/smv-init MyApp com.mycompany.myapp
```

## Build and Run Example App

```shell
$ mvn clean install
$ _SMV_HOME_/tools/smv-run --run-app
```

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

See [Getting Started](docs/user/getting_started.md) section of User Guide for further details.

## Generate dependency graph

If `smv-run` is provided the `-g` flag, instead of running and persisting the module, the module dependency graph will be created as a `dot` file. It can be converted to `png` using the `dot` command.

```scala
$ _SMV_HOME_/tools/smv-run -g -m com.mycompany.myapp.stage1.EmploymentByState
$ dot -Tpng com.mycompany.MyApp.stage1.EmploymentByState.dot -o graph.png
```

See [Run SMV Application](docs/user/run_app.md) for further details.
