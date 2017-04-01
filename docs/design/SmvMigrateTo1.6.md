# Key changes

* `first` returns the real first instead of the first-non-null as before
* `AggregateExpression1` doesn't exist anymore
* Internal `First` expression actually taking an additional parameter `ignoreNull` which will specify the behavior of how to deal wth nulls
* Added `stddev` aggregate function

# Changes made in SMV 1.5.2 for easy magration

* Removed CDS and SmvLocalRelation
* `dedupByKey` is using GDO instead of `SmvFirst`

# Changes need to be made to compile on 1.6
* Change `smvFirst` function to use 1.6's `First` expression
* Rename our version of `stddev`

# possible client code change
No
