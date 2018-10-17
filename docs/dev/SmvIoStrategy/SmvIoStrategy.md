# Generic Module IO Strategy

There are circumstances in which it would be useful for an `SmvModule` to return a result which is not a `DataFrame`. For example, when using SMV for modeling it would be helpful to train the model in one module and score data with the model in another module downstream. In this scenario, we would also like to persist the model as an intermediate result as we would with a `DataFrame`.

We currently handle the "model" special case by converting the model result into a string (by pickle) and stuffing that into a single row `DataFrame`.
This approach will not scale to other types well.  For example, we would like to support "generic" modules that may depend-on and return types such
as Pandas data, H2O data, none-serializable models, etc.

In addition to the persistence issue, we should utilize the same method to persist "published" results as well as intermediate result persistence.

# Solution
The solution is to delegate the persistence/publishing of all module results into a concrete implementation of `SmvIoStrategy` class.
`SmvIoStrategy`s would be responsible for persisting/reading result data.  The `SmvModule` will no longer assume results/inputs are Spark `DataFrame`.

Each concrete `SmvModule` type would have to specify the corresponding `SmvIoStrategy` class that can handle the result of it's `run` method.
The current `SmvModule` can continue to be specific to Spark modules and can be bound to `SmvCsvOnHdfsIoStrategy` result.
Other modules types can specify different io strategies.
For example, `SmvPandasModule` can specify `SmvPandasIoStrategy` as it's strategy type.
Downstream modules that depend on a instance of `SmvPandasModule` can assume that they will receive a pandas `DataFrame` as input.

Note: if a module is not ephemeral, it **must** define an `persistStrategy` method that returns a valid strategy.  Ephemeral modules can ignore that method.

## Metadata
Similar to the persistence of `run` result output, a module can specify an IO strategy to be used to persist the metadata using the `persistMetadataStrategy` method.
By separating the `run` strategy from the `metadata` strategy, a module can persist the raw data in a completely different place than the metadata.
For example, the hive output module may persist the raw data output to a hive table but persiste the metadata to a json file on HDFS.

## `SmvIoStrategy` implementation
Since the concrete dataset class needs to provide an instance of `SmvIoStrategy`, we can use the constructor of the strategy to capture the variant
in the required arguments to the strategy.  For exmaple, HDFS strategy may require a directory whereas a hive strategy may require a schema and
table name.  These areguments can be supplied to the constructor as the caller (concrete dataset) will know what is required and will have the info.  This has two advantages:
* Do not have to make a kitchen sink interface on dataset to support all possible information needed by strategies.
* Can easily extend the types of datasets and strategies in the future without changing core SMV

From the io strategy point of view, there is no difference between intermediate data persist and publishing.
IO strategey classes that do both (e.g. `SmvCsvOnHDFSIoStrategy`) will take a constructor parameter to indicate
if this instance is being used for persit or publish.

An instance of `SmvIoStrategy` can also be created in the `run` method of `SmvOutput` modules.  It not limited to being constructed in `persistStrategy` method.

## Exmaple IO strategy implementations
```python
class SmvCsvOnHdfsIoStrategy(SmvIoStrategy):
  def __init__(publishVersion, hash, ...):
    ...
  def write(rawData):
    if (publishVersion):
      # write to publish directory withtout hash in filename
    else:
      # write to output directory using hash file name
  def read():
    ...

class SmvJsonOnHdfsIoStrategy(SmvIoStrategy):
  """read/write json to HDFS.  For example for metadata persistence"""
  def __init__(hash, ext, ...):
    ...
  def write(rawData):
    # rawData here means the json object.
    ...
  def read():
    ...

class SmvHiveIoStrategy(SmvIoStrategy):
  """read/write to spark DF to hive tables.
     can only be used for publishing"""
  def __init__(schemaName, tableName):
    ...
  def write(rawData):
    # write rawData to the specified schema/table (no hash/publish)
```

## Example client code
```python
class M1_S(SmvModule):
   """This is a spark dataframe module"""
   def run(i):
     return DataFrame(...) # spark dataframe

class M2_P(SmvPandasModule):
   """This is a pandas dataframe module"""
   def run(i):
     return pandas.createDataFrame(...) # pandas dataframe

class M3_J(SmvModule):
   """spark module that combines pandas+spark dataframes"""
   def requiresDS(): return [ M1_S, M1_P ]
   def run(i):
      s = i[M1_S] # this is a spark DataFrame
      p = i[M1_P] # this is a pandas DataFrame

      return s.join(p.toSpark(), ...) # spark dataframe
```

**Note:** user code doesn't know or see any reference to `SmvIoStrategy`.
By the time the `run()` method is called, the raw results are extracted by `SmvIoStrategy`

## Example dataset definition
```python
class SmvModule(SmvDataSet):
  def persistStrategy():
    return SmvCsvOnHdfsIoStrategy(publishVersion=None, hash=..., ...)
  def persistMetadataStrategy():
    return SmvJsonOnHdfsIoStrategy(hash=..., ext=".meta")

class SmvPandasModule(SmvDataSet):
  def persistStrategy():
    return SmvPandasIoStrategy(outputDir="...", hash=...)

class SmvOutput:
  """by default, output modules do not persist result or metadata"""
  def persistStrategy(): return None
  def persistMetadataStrategy(): return None

class SmvHiveOutput(SmvOutput):
  """hive output module.
     note that we create the strategy in the run method.
     Derived classes just need to define the `requiresDS`."""
  def isEphemeral(): return True
  def run(i):
    s = SmvHiveIoStrategy("myschema", "mytable")
    s.write(i["input"])
    return ???
```

# TBD:
* how to determine what module can link to another module.  For example, a CSV output module should only accept a module who produced spark data frame as output.  May need to create an output/input type declration so we can do programmatic check on valid links.
* What should "output" modules return from `run`.  Should they be chainable or should they always be leaf nodes.

