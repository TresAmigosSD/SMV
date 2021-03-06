# SMV validator
Current DQM solution can be used to validate the current result of a run of a module.
This proposal is to provide the user a method to validate run results against historical
run data.  The user is responsible for specifying what meta data to store per run
and SMV is responsible for maintaining the historical data and making it available
to the validator.

There are two major components to the validator.
1. The metadata generation API.
2. The validation API

**Note:** the validator interface will only be implemented in python for now.

---
# API

## Metadata Generation API
User should be able to provide a method that will return a per run metadata.
The return should be a simple python dictionary that can be converted to json.
The method should accept the result of the Dataset run method (i.e. the result
`DataFrame`).  The user should also be able to specify how much metadata should be
persisted (how many historical runs).  The default should be the last 5 runs.

## Validation API
User should be able to provide an optional `validate` method on a Dataset that will
be used to validate the metadata results.  The method will be given the metadata
of the current run as well as an array of historical metadata.  If an exception
is **not** thrown by the `validate` method, then the validation is assumed to
pass.  If any exception is thrown, then the validation fails and the run is stopped.

## Example
```python
class MyModule(SmvModule):
  def requireDS(self): return [A,B]
  def run(self, i):
    ... process A+B to generate DF result...

  # number of run history to maintain.  Default to 5?
  def metadata_hist_size(self):
    return 10

  # The result of the run is provided to the `metadata` method.  The method should
  # return a serializable python dictionary object.
  def metadata(self, df):
    id_count = df.select("id").distinct().collect()[0][0]
    return { "id_count": id_count }

  # The validate method is provided the metadata object generated by the current
  # run as well as an array of historical metatdata (which might be empty)
  def validate(self, cur_metadata, hist_metadata):
    # if we don't have history, nothing to compare
    if len(hist_metadata) == 0: return

    # compute average historical id count.
    avg_id_count = sum(hist_metadata.id_count) / len(hist_metadata)

    # if current id count is > %10 different than historical, fail validation.
    if abs(avg_id_count - cur_metadata.id_count)/avg_id_count > 0.1:
      raise "Large Difference in id_count.  avg = ?, cur = ?"
```
---

# Implementation details
The metadata will be persisted in a per Dataset file (non-versioned).  The metadata
will consist of the last N run results.  Since the file will possibly be in
HDFS, SMV will have to:
* read the historical metadata from metadata file.
* in-memory append the new metadata for current run (includes user and builtin metadata)
* trim old metadata if needed (more than N runs in history)
* delete old metadata file.
* write new N records to metadata file.

**Note:** should also provide a method that will return the available metadata for
a given module FQN.  External applications can then use that method to extract
metadata of a known FQN.

## Built-in metadata
In addition to the user defined metadata above, each Dataset will automatically
generate default metadata such as running time, number of output records, etc.
