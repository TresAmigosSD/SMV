# TODO: Need to update this.

# TODO: Add link to the getting_started page on github!!!


# An Example App for Quick Start

This is a dummy app on US employment data. Please see `data/input/employment/info.md` for basic 
data info. 

With SMV package installed, you can compile and then run this App on the Spark Shell.


## Explore SmvFile and SmvModules 


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


