# Spark Modularized View (SMV)
Spark Modularized View enables users to build enterprise scale applications on Apache Spark platform.

## Why SMV
* Scales with **DATA** size
* Scales with **CODE** size
* Scales with **TEAM** size

In addition to the data scalability inherited from Spark, SMV also provides code and team scalability through
the following features:
* Multi-level modular design allow developers to work on large scale projects, and enable easy code and data reuse
* Multi-grain traceability to support full scope knowledge transparency to developers and data users
* Provides interfaces to multiple languages(Scala and R for now) for easy integrating to existing code and leverage existing developer experiences
* Pure text code, can utilized modern CM (Configuration Management) tool to track and merge changes among team members
* Automatic Data and Code version synchronization to enable coordination on both code and data level
* Data publishing mechanism to support inter-team coordination
* Build-in data quality management to ensure data quality in a continuous bases
* High level helper functions and tools for quick data App development

Please refer to [User Guide](docs/user/0_user_toc.md) and
[API docs](http://tresamigossd.github.io/SMV/scaladocs/index.html#org.tresamigos.smv.package) for details.

:cherries::cherries: Please see [***Important Announcment***](docs/announcement.md) for latest update which may impact users' projects or scripts.

**Note:** The sections below were extracted from the User Guide and it should be consulted for more detailed instructions.

# Use SMV with Docker

## Docker
Install [Docker](https://www.docker.com/what-docker). An installation guide for your machine may be found [here](https://docs.docker.com/engine/installation/).

## For users
The first time you run the smv Docker image, Docker will download it for you automatically. You need to tell Docker where to find your projects directory and your data directory. You will enter a shell with all SMV tools installed. Find your projects in /projects and your data in /data.  Note that both the projects and data directories **must** already exist on the host system.
```shell
$ docker run -it -v /path/to/projects:/projects -v /path/to/data:/data tresamigos/smv
```

## For developers

The smv-core image contains only the tools needed for SMV development, so you may build SMV from source with mvn or sbt. Find your SMV source in /smv and your projects in /projects in the container.

Run smv-core.sh from \_SMV_HOME\_/docker/smv-core

```shell
_SMV_HOME_/docker/smv-core/$ ./smv-core.sh /path/to/projects
```

or any other directory

```shell
/any/other/directory$ _SMV_HOME_/docker/smv-core/smv-core.sh /path/to/projects /path/to/smv
```

# SMV Getting Started

## Create example App

SMV provides a shell script to easily create an example application.
The example app can be used for exploring SMV and it can also be used as an initialization script for a new project.

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
