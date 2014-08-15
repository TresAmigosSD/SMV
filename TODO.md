Schema
------
* add support for adding/ignoring header column in csv file.

Aggregates
----------
* investigate running aggregate
* partition-by
* implement all needed aggregate functions (e.g. Sum0, Any, ...)

Framework
---------
* investigate "mv" command vs. catalyst.
* investigate using Option\[Double] or equiv for handling null prop/ignore
    * Rather than Option, derive from Numeric\[T] and define the plus, mult, ops.
    * investigate using ValueClass (hard coded type) to avoid autoboxing (can not use Numeric[T] as that is not @speicalized as of scala 2.10)
* UDI like operators (inline filter)

* Allow labels in schema entry (should labels part of the syntax?)
* DQM framework
* Generate dependency graph (rdd and variable level)
* Add collection types in schema entry

EDD
---
* create helper method at SchemaRDD level to impute null values (e.g. all missing doubles are 0)
    * perhaps allow user which columns the imputation should happen (regex, list, etc).

Testing
-------

Documentation
-------------
* SchemaRDD loader/writer doc
* Readme
