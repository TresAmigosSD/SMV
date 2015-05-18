# An Example App for Quick Start

This is a dummy app on US employment data. Please see `data/input/employment/info.md` for basic 
data info. 

With SMV package installed, you can compile and then run this App on the Spark Shell.

## Compile
```shell
$ cd /pathToThisExample/01GetStart
$ mvn package
```

For some system `mvn3` is the Maven command instead of `mvn`.

## Start Spark Shell with SMV
First, you need to check the Spark shell command path in the `shell/run.sh` file. Make the 
change to make it refer to the Spark Shell command location in your environment.

```shell
$ ./shell/run.sh data
```

You should see something like,
```
Welcome to
      ____              __
     / __/__  ___ _____/ /__
    _\ \/ _ \/ _ `/ __/  '_/
   /___/ .__/\_,_/_/ /_/\_\   version 1.3.0
      /_/

Using Scala version 2.10.4 (Java HotSpot(TM) 64-Bit Server VM, Java 1.7.0_71)
Type in expressions to have them evaluated.
Type :help for more information.
Spark context available as sc.
SQL context available as sqlContext.
Loading shell/shell_init.scala...
import org.apache.spark.sql._
import functions._
import org.apache.spark.rdd.RDD
import org.tresamigos.smv._
import org.tresamigos.getstart._
import core._
import etl._
app: org.tresamigos.getstart.core.ExampleApp = org.tresamigos.getstart.core.ExampleApp@3e00cc04
sqlContext: org.apache.spark.sql.SQLContext = org.apache.spark.sql.SQLContext@5bf9457c
import sqlContext.implicits._
defined module i
import i._

scala> 
```

You can check `shell/shell_init.scala` for the preloaded variables and methods.

## Discover Schema of the Data 
Before we can load the data as SmvFile, we need to create the Schema file. the preloaded 
method `findSchema` will help you to create the Schema file.

Within the Spark Shell 
```scala
scala> findSchema("data/input/employment/CB1200CZ11.csv")(p.caBar)
```

The string in the first parameter list is the path of the data file. It is a relative path 
to the running dir, which is 01GetStart for this project.

The parameter in the second parameter list, `p.caBar`, specifies the CSV attributes for the 
input data. `p` is a brief name of the ExampleApp object, which is defined in 
`src/main/scala/org/tresamigos/getstart/core/ExampleApp.scala`. 

The command will create a Schema file with the same path of the data file, with `schema` postfix.
The contents of that file is based on the best guess of the Schema of the data. You may want to 
make some manual fix before use the data.

## Explore SmvFile and SmvModules 

For the input data, you can check it as below
```scala
scala> val d1=s(p.employment)
scala> d1.printSchema
scala> d1.show(1)
scala> d1.select("ZIPCODE", "YEAR", "ESTAB", "EMP").show(10)
```

The output of last command will show something like
```
ZIPCODE YEAR ESTAB EMP  
35004   2012 167   2574 
35005   2012 88    665  
35006   2012 20    0    
35007   2012 596   10347
35010   2012 463   6725 
35011   2012 30    0    
35013   2012 3     0    
35014   2012 23    106  
35015   2012 4     0    
35016   2012 313   3306 
```

You can also access SmvModules defined in the code.
```
scala> val d2 = s(EmploymentRaw)
12:19:16 PERSISTING: data/output/org.tresamigos.getstart.etl.EmploymentRaw_0.csv
12:19:17 RunTime: 1 second and 545 milliseconds                                                    
d2: org.apache.spark.sql.SchemaRDD = [EMP: int]
```

`EmploymentRaw` is defined in the `etl` package `Employment.scala` file.
As you can see above, when you try to refer to a SmvModule, it will do the calculation and
then persist it for future use. Now you can use `d2`, a DataFrame, to refer to the 
SmvModule output, although in this example, there are nothing intersecting in that 
data other than the `EMP` field.

## Run EDD on data
To quickly get an overall idea of the input data, we can use the SMV EDD tool.
```scala
scala> d1.select("ZIPCODE", "YEAR", "ESTAB", "EMP").edd.addBaseTasks().addHistogramTasks("ESTAB", "EMP")().dump
```

You will see something like the following as the output.
```
Total Record Count:                        38818
ZIPCODE              Non-Null Count:        38818
ZIPCODE              Average:               49750.09928383732
ZIPCODE              Standard Deviation:    27804.662445936523
ZIPCODE              Min:                   501
ZIPCODE              Max:                   99999
YEAR                 Non-Null Count:        38818
YEAR                 Average:               2012.0
YEAR                 Standard Deviation:    0.0
YEAR                 Min:                   2012
YEAR                 Max:                   2012
ESTAB                Non-Null Count:        38818
ESTAB                Average:               191.45262507084348
ESTAB                Standard Deviation:    371.37743343837866
ESTAB                Min:                   1
ESTAB                Max:                   16465
EMP                  Non-Null Count:        38818
EMP                  Average:               2907.469241073725
EMP                  Standard Deviation:    15393.485966796263
EMP                  Min:                   0
EMP                  Max:                   2733406
Histogram of ESTAB: with BIN size 100.0
key                      count      Pct    cumCount   cumPct
0.0                      26060   67.13%       26060   67.13%
100.0                     3129    8.06%       29189   75.19%
200.0                     1960    5.05%       31149   80.24%
300.0                     1445    3.72%       32594   83.97%
400.0                     1136    2.93%       33730   86.89%
500.0                      937    2.41%       34667   89.31%
600.0                      820    2.11%       35487   91.42%
700.0                      616    1.59%       36103   93.01%
800.0                      552    1.42%       36655   94.43%
900.0                      422    1.09%       37077   95.51%
1000.0                     338    0.87%       37415   96.39%
1100.0                     314    0.81%       37729   97.19%
......
-------------------------------------------------
Histogram of EMP: with BIN size 100.0
key                      count      Pct    cumCount   cumPct
0.0                      15792   40.68%       15792   40.68%
100.0                     3132    8.07%       18924   48.75%
200.0                     1844    4.75%       20768   53.50%
300.0                     1235    3.18%       22003   56.68%
400.0                      988    2.55%       22991   59.23%
500.0                      738    1.90%       23729   61.13%
600.0                      664    1.71%       24393   62.84%
700.0                      567    1.46%       24960   64.30%
800.0                      466    1.20%       25426   65.50%
......
```

Please refer SMV document on EDD for more details.

## Create Your Own Project

You can use this project as a template to create your own project. You need to 
make some changes on the files to reflect your orginzation name and project name.

### pom.xml
Those 2 lines,

```xml
  <groupId>org.tresamigos</groupId>
  <artifactId>getstart</artifactId>
```

### shell/run.sh
Rename the `getstart` in this line,
`./target/getstart-1.0-SNAPSHOT-jar-with-dependencies.jar`,
to your project name.

### shell/shell_init.scala
Rename the package name in this line
```
import org.tresamigos.getstart._, core._ , etl._
```

### Rename the directory 
```
src/main/scala/org/tresamigos/getstart/
```

### core/ExampleApp.scala
Rename package name and 
```scala
  override def getModulePackages() = Seq(
    "org.tresamigos.getstart.etl",
    "org.tresamigos.getstart.adhoc"
  )
```
to include your project packages.

Also change the file references in `object ExampleApp`,
```scala
val employment = SmvCsvFile("input/employment/CB1200CZ11.csv", caBar)
```

### etl/*scala
Rename the package names. 

Please refer to SMV document on "Application Framework" for more details on where to 
go from this point.
