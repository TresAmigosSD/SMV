# Re-Organize Python Side SmvDataSet

The class structure of SmvDataSet and its children classes are the core of
SMV. Since we decided future development will focus on Python side, it's
time to put the Scala side class structure aside and re-consider what is the best
way to organize the classes on the Python side.

## Top level structure

```
SmvDataSet -->
  - SmvInputBase
  - SmvModule
  - SmvOutputBase
```

## SmvInputBase

SmvInputBase is the base class of all input DS. It is almost the same as current
`SmvInput`, but with the following changes:

* Adding an abstract method `readAsDF`
* Remove the `getRawScalaInputDS` stuff

Class `SmvInputWithScalaDS` will be derived from `SmvInputBase`, and be the
parent class of all the current built-in input modules.

### Future and User-defined inputs

All future or user defined input module should be derived from `SmvInputBase`
directly or indirectly.

Basically the minimal requirement of defining one concrete class of
`SmvInputBase` is to specify the `readAsDF` method. Optionally, user can
also specify `instanceValHash` so that the hash-of-hash can depend on the the
source data.

In longer run, we will move all the existing built-in input modules to
directly under `SmvInputBase` and drop dependency to Scala side. It should be
pretty easy for the tables (`SmvHiveTable`, `SmvJdbcTable`), but nontrivial
for the CSV inputs (`SmvCsvFile`, `SmvMultiCsvFiles`, `SmvCsvStringData`).

For CSV files, SMV's validator mechanism is still better than spark's csv
reader. However reading some DataBrick's stuff, it is quite promising that
in the future rejected records will output. In that case, we may eventually
be able to drop our own CSV reader (still performance need to be checked).

## SmvOutputBase
TO BE ADDED
