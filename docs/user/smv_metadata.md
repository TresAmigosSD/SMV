# Module Metadata

After an `SmvModule` is run, it can be helpful to have a record of what its inputs were, when it was run, etc. Metadata associated with an `SmvModule` can be found in a `.meta` directory sibling to its `.csv` and `.schema` outputs. Currently this metadata includes a list of the module's columns and column metadata, a list of its inputs (including their versions at the time) and a timestamp marking when the module was run. The metadata is formatted as minified Json. To inspect the data, use Python's `json.tool` module like

```bash
$ cat data/output/my.smv.module_123abcd.meta/* | python -m json.tool
```
