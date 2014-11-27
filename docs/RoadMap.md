# Variable Meta Data support
For variables to be shared with multiple teams, the meta information about them need to be shared too. In ideal case, the code should carry those meta data along with data schema. SMV will support variables with meta data through a 2-phase plan.

## Short term solution
Implement meta data within SmvSchena class, and wrap SchemaRDD with a wrapper class, which attaches an SmvSchema. Cleint code is documented in issue #103.

## Long term solution
In the ideal situation, one would add the description when defining the variable (as part of select or groupByKey).  For example
```scala
srdd.select('a + 'b as 'c with ('description -> "c is the sum of a and b")
```
OR
```scala
srdd.select('a + 'b as 'c documentation "blah blah")
```
Basically, we would need to extend NamedExpressions to support our extra expression info.  We would then propagate that to result.  The "with" syntax would also allow for other various meta-data to be attached to an expression (not just the variable).

We wouldn't be modifying the spark internals.  We would need to create a new class that inherits NamedExpression (e.g. NamedExpressionWithMetaData) that just adds the meta info.  We would also implement an implicit "with" method on NamedExpression that accepts one or more (Symbol, ?) tuples.

We can add our own Strategy classes for propagating the meta data (different rules for different meta data).  For example, some meta data should be "inherited" (e.g. how to handle null propagation), while some property is fixed for that expression (e.g. the documentation string).
