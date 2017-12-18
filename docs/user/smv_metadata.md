# Module Metadata

After an `SmvModule` is run, it can be helpful to have a record of what its inputs were, when it was run, etc. Metadata associated with an `SmvModule` can be found in a `.meta` directory sibling to its `.csv` and `.schema` outputs. Currently this metadata includes a list of the module's columns and column metadata, a list of its inputs (including their versions at the time) and a timestamp marking when the module was run. The metadata is formatted as minified Json. To inspect the data, use Python's `json.tool` module like

```bash
# cat data/output/my.smv.module_123abcd.meta/* | python -m json.tool
```

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
