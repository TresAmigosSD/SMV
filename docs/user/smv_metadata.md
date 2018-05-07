# Module Metadata

After an `SmvModule` is run, it can be helpful to have a record of what its inputs were, when it was run, etc. Metadata associated with an `SmvModule` can be found in a `.meta` directory sibling to its `.csv` and `.schema` outputs. Currently this metadata includes a list of the module's columns and column metadata, a list of its inputs (including their versions at the time) and a timestamp marking when the module was run. The metadata is formatted as minified Json. To inspect the data, use Python's `json.tool` module like

```bash
# cat data/output/my.smv.module_123abcd.meta/* | python -m json.tool
```

# Built-in metadata

SMV includes the following fields in all module metadata:
- `_fqn`: fully qualfied name of the module
- `_columns`: the name, data type, and metadata of each column of the module's output
- `_inputs`: all of the immediate dependencies of the module
- `_timestamp`: the time at which the module was run (all modules run in one transaction will share a timestamp)
- `_duration`: the amount of time in seconds spent on different tasks while running the module
    - `metadata`: time spent generating user metadata
    - `dqm`: time spent running dqm validation. This should be close to 0s if the module isn't ephemeral, as the rules and fixes are applied while persisting the module. Otherwise, the time will be dominated by time spent counting rule failures and fixes.
    - `persisting`: time spent persisting output to csv. This field will be omitted if the module is ephemeral, as output will not be persisted. Otherwise, this will include time spent counting rule failures, which is simultaneous with persisting.

# Custom metadata

SMV's Python API gives you the option to add custom metadata to your module's automatically generated metadata. To do so, override your module's `metadata` method to return a `dict` of metadata. `metadata` takes the result of your module as an argument

**Python**
```python
class MyMod(smv.SmvModule):
  ...
  def metadata(self, df):
    record_count = df.count()
    return {"record_count": record_count}
```

# Metadata Validation

You can also validate any of your module's metadata as part of DQM by overriding the `validateMetadata` method, which takes the current metadata and an list of saved historical metadata as an argument. To indicate an validation failure, `validateMetadata` should return a message describing the error. By default SMV saves metadata from the last 5 runs. Override `metadataHistorySize` to change this number.

**Python**
```python
class MyMod(smv.SmvModule):
  ...
  def validateMetadata(self, current, history):
    if current['record_count'] < min(m['record_count'] for m in history):
      return "not enough records!"

  def metadataHistorySize(self):
    return 10
```

# Mix-in metadata validators
It is somewhat onerous on the dataset developer to maintain the `metadata`/`validateMetadata`
pairing for each dataset.  This is especially true when multiple rules need to be
applied on a dataset.

SMV provides a higher order metadata validation interface that allows the dataset
author to "mix-in" pre-defined validation rules.  The validation rules have a
similar interface to the raw `metadata`/`validateMetadata` interface.  Except that
they can be mixed in into dataset definitions.

## Defining a validator
Mix-in validators need to inherit from `SmvHistoricalValidator` base class.  Each
validator should then define a `metadata` and `validateMetadata` methods as before.

**Note:** It is essential that the derived validator class call the super `__init__`
method with the parameters of the validators.  This will determine the "key" of the
validator instance.  The "key" will be used to store the validator instance metadata
results in the full dataset metadata.

```python
import smv

class DistinctCountVariation(smv.SmvHistoricalValidator):
    """Check the distinct count of a given column against average historical count.
       Parameters:
         col : column name to compute distinct count.
         threshold : percent variation allowed.  Range [0,1]
    """
    def __init__(self, col, threshold):
        super(DistinctCountVariation, self).__init__(col, threshold)
        self.col = col
        self.threshold = threshold

    def metadata(self, df):
        count = df.select(self.col).distinct().count()
        return {"d_count": count}

    def validateMetadata(self, cur, hist):
        if len(hist) == 0: return None
        hist_count_avg = float(sum([h["d_count"] for h in hist])) / len(hist)
        cur_count = cur["d_count"]

        #print ("hist_count_avg = " + str(hist_count_avg))
        #print ("cur_count = " + str(cur_count))
        if (float(abs(cur_count - hist_count_avg)) / hist_count_avg) > self.threshold:
            return "DistinctCountVariation: count = %d, avg = %g" % (cur_count, hist_count_avg)
        return None
```

## Using a custom validator
Once the custom validators have been defined, they can be "mixed-in" into an `SmvDataset`.
Multiple instances of a validator class can be mixed-in into the same dataset.
For example, the same distinct count variation validator above can be mixed in for
different columns.

The validators are "mixed-in" using the `SmvHistoricalValidators` decorator on the
dataset class.  One or more validators can be passed as parameters to `SmvHistoricalValidators`.

```python
@smv.SmvHistoricalValidators(
    DistinctCountVariation("ST", 0.25),
    ValueRangeVariation("EMP", 0.10)
)
class EmploymentByState(smv.SmvModule, smv.SmvOutput):
    """Python ETL Example: employ by state"""

    def requiresDS(self):
        return [Employment]

    def run(self, i):
        df = i[Employment]
        return df.groupBy(F.col("ST")).agg(F.sum(F.col("EMP")).alias("EMP"))

```

In the above example, the `EmploymentByState` module will have two additional validators
added to it.  The `"ST"` column will be checked against variation in distinct count
(up to %25 variation will be acceptable).  Whereas the min/max of the `"EMP"` column
will be check for variation up to %10.
