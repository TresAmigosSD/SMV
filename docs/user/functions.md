# SMV Helper Functions

SMV provides a variety of functions to aid in the development of applications.
The provided functions come in one of three flavors.

* `Column` helper functions
* `DataFrame` helper functions
* Grouped `DataFrame` helper functions

## Column Helper Functions
This set of functions can be applied to a column inside a projection.
For example,
```scala
df.selectPlus(
  $"a".smvSoundex as 'a_soundex,
  $"ts".smvYear as 'year,
  ...)
```

See [SMV ColumnHelper API docs](http://tresamigossd.github.io/SMV/scaladocs/index.html#org.tresamigos.smv.ColumnHelper) for more details.

## DataFrame Helper Functions
[DataFrame helper functions](funcs_dataframe.md) can be used on a `DataFrame` directly.

## Grouped Helper Functions
[Grouped helper functions](funcs_grouped.md) can only be applied to `SmvGroup`ed data.

TODO: also need link to functions that are not column helpers such as 'SmvStrCat'