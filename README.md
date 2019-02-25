
<img height="128" src="https://github.com/TresAmigosSD/SMV/raw/master/docs/images/smv-logo-100px.png"/>

# Spark Modularized View (SMV)

[![Build Status](https://travis-ci.org/TresAmigosSD/SMV.svg?branch=master)](https://travis-ci.org/TresAmigosSD/SMV)
[![Join the chat at https://gitter.im/TresAmigosSD/SMV](https://badges.gitter.im/TresAmigosSD/SMV.svg)](https://gitter.im/TresAmigosSD/SMV?utm_source=badge&utm_medium=badge&utm_campaign=pr-badge&utm_content=badge)


Spark Modularized View enables users to build enterprise scale applications on Apache Spark platform.

* [Quick Start](#smv-quickstart)
* [User Guide](docs/user/0_user_toc.md)
* [Python API docs](http://tresamigossd.github.io/SMV/pythondocs/2r10/index.html)

# SMV Quickstart

## Installation

### Pip Install

SMV is now [distributed as a package on PyPi](https://pypi.org/project/smv/). It comes in two flavors -- with and without a dependnecy on `pyspark`. The first is for consumers who might be installing to a machine outside of a cluster that does not already have `pyspark` installed, while the second is targeted for those installing to a gateway machine in a cluster that already has Spark available in the environment.

#### Without Pyspark

```bash
pip install smv
```

#### With Pyspark

```bash
pip install smv[pyspark]
```

### Docker

We avidly recommend using [Docker](https://docs.docker.com/engine/installation/) to install SMV. Using Docker, start an SMV container with

```
docker run -it --rm tresamigos/smv
```

If Docker is not an option on your system, see the [installation guide](docs/user/smv_install.md).

## Create Example App

SMV provides a shell script to easily create template applications. We will use a simple example app to explore SMV.

```shell
$ smv-init -s MyApp
```

## Run Example App

Run the entire application with

```shell
$ smv-run --run-app
```

This command must be run from the root of the project.

The output csv file and schema can be found in the `data/output` directory. Note that 'XXXXXXXX' here substitutes for a number which is like the version of the module.

```shell
$ cat data/output/stage1.employment.EmploymentByState_XXXXXXXX.csv/part-* | head -5
"50",245058
"51",2933665
"53",2310426
"54",531834
"55",2325877

$ cat data/output/stage1.employment.EmploymentByState_XXXXXXXX.schema/part-*
@delimiter = ,
@has-header = false
@quote-char = "
ST: String[,_SmvStrNull_]
EMP: Long
```

## Edit Example App

The `EmploymentByState` module is defined in `src/python/stage1/employment.py`:

```shell
class EmploymentByState(SmvModule, SmvOutput):
    """Python ETL Example: employment by state"""

    def requiresDS(self):
        return [inputdata.Employment]

    def run(self, i):
        df = i[inputdata.Employment]
        df1 = df.groupBy(col("ST")).agg(sum(col("EMP")).alias("EMP"))
        return df1
```

The `run` method of a module defines the operations needed to get the output based on the input. We would like to filter the table based on if each row's state is greater or less than 1,000,000. To accomplish this, we need to add a filter to the `run` method:

```shell
  def run(self, i):
      df = i[inputdata.Employment]
      df1 = df.groupBy(col("ST")).agg(sum(col("EMP")).alias("EMP"))
      df2 = df1.filter((col("EMP") > lit(1000000)))
      return df2
```

Now run the module again with

```shell
smv-run --run-app
```
(make sure you run this from the from the root of the project)

Inspect the new output to see the changes.

```shell
$ cat data/output/stage1.employment.EmploymentByState_XXXXXXXX.csv/part-* | head -5
"51",2933665
"53",2310426
"55",2325877
"01",1501148
"04",2027240

$ cat data/output/stage1.employment.EmploymentByState_XXXXXXXX.schema/part-*
@delimiter = ,
@has-header = false
@quote-char = "
ST: String[,_SmvStrNull_]
EMP: Long
```

### Publish to Hive Table

If you would like to publish your module to a hive table, add a `tableName` method to EmploymentByState. It should return the name of the Hive table as a string.

```python
class EmploymentByState(SmvModule, SmvOutput):
    ...
    def tableName(self):
        return "myTableName"
    def requiresDS(self): ...
    def run(self, i): ...
```

Then use
```bash
$ smv-run --publish-hive -m stage1.employment.EmploymentByState
```

## smv-pyshell

We can also view the results in the smv-pyshell. To start the shell, run

```
$ smv-pyshell
```

To get the `DataFrame` of `EmploymentByState`,

```shell
>>> x = df('stage1.employment.EmploymentByState')

```

To peek at the first row of results,

```shell
>>> x.peek(1)
ST:String            = 50
EMP:Long             = 245058
cat_high_emp:Boolean = false
```

See the [user guide](docs/user/0_user_toc.md) for further examples and documentation.



# Contributions

Please see [SMV Development Best Practices](docs/dev/00_DevProcess/best_practice.md).
