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
df.select(
  $"a".smvSoundex as 'a_soundex,
  $"ts".smvYear as 'year,
  ...)
```

See [SMV ColumnHelper API docs](http://tresamigossd.github.io/SMV/scaladocs/index.html#org.tresamigos.smv.ColumnHelper) for more details.

## DataFrame Helper Functions
This set of functions can be applied to an existing `DataFrame`.
For example:
```scala
df.selectPlus( $"amt" * 2 as "double_amt")
```
Can be used to add an additional column to a `DataFrame`.

See [SmvDFHelper API docs](http://tresamigossd.github.io/SMV/scaladocs/index.html#org.tresamigos.smv.SmvDFHelper) for more details.

## Grouped Helper Functions
[Grouped helper functions](funcs_grouped.md) can only be applied to `SmvGroup`ed data.

TODO: also need link to functions that are not column helpers such as 'SmvStrCat'