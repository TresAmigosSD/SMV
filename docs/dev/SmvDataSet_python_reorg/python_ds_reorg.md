# Re-Organize Python Side SmvDataSet

The class structure of SmvDataSet and its children classes are the core of
SMV. Since we decided to do future development focused on Python side, it's
time to put the Scala side class structure and re-consider what is the best
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

## SmvOutputBase
TO BE ADDED
