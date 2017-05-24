# sourceCodeHash and instanceValHash

For most modules, `datasetHash` is just a CRC of the source code for the module's class definition. However, for certain input datasets the `datasetHash` also depends on some instance values, like the timestamp on the CSV target of an `SmvCsvFile`. For the Python port of datasets like `SmvCsvFile`, we want the Python port must implement the source code hash but should not duplicate any knowledge about the hash of instance values. Therefore, we will reimplement `datasetHash` as the sum of an `instanceValHash` method and a `sourceCodeHash` method. `SmvExtModulePython` will implement `sourceCodeHash` and `instanceValHash` instead of `datasetHash`.

## sourceCodeHash

`sourceCodeHash` will depend only on the source code of the module's class definition. This requires only a single Python definition and a single Scala definition.

## instanceValHash

`instanceValHash` will depend on the instance values of an `SmvDataSet`. For most `SmvDataSets`, like `SmvModule`, this `instanceValHash` will always be zero. To avoid duplicating code, **non-zero implementations of instanceValHash will always be in Scala**. All Python `SmvDataSets` which currently need a non-zero (i.e. the `SmvFiles`) `instanceValHash` already own their own Scala counterparts, to which they can defer for `instanceValHash`.
