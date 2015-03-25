# SmvSchema, SmvModule, DataFrame 

In additional to the content and function DataFrame/SchemaRDD support, we need to support the following 

* Automatically data persist and versioning
* CSV and FRL data handling, including formating on write
* Easy access to example records. May need to support a single record, or top 100 record
* Column level meta data (field descriptions and other labels)
* RDD level meta data (runTime, Count, EDD)

## Currently Design

Current SMV (Spark 1.1) has SmvSchema (class name Schema) and SmvModule. Where SmvSchema supports

* String -> Value, Value -> String for CSV & FRL I/O
* Data Type info which in general repeat StructField of SparkSQL to give SMV access to Spark private info of the types
* Schema file read write, currently only field name and type

SmvModule supports

* Data persisting as file automatically under debug mode
* Persisted file versioning
* Module level dependency

## Propagate or Just-need-at-output

For the additional functions, we need to answer 

* Does it attach to SRDD (Schema) or SmvModule
* If it's on SRDD level, whether the requirement is on the output level or have to be propagated from one SRDD to another

It is clear that persisting and versioning is on SmvModule. RDD level meta data should actually be on SmvModule, so it's should be considered "SmvModule" level meta data.

CSV and FRL handling and formating should be clearly at SmvSchema level. However propagating format is not well-defined and unnecessary.

The use case for single record example is mainly for early stage of data ETL, where we need to figure out data type from examples. Although we have Schema Discovery tool, it's needed to make some human review and adjustment. A single record example beside the schema is very helpful. Then the question is do we really need this single record after Schema Discovery. 

The use case of top 100 is mainly from the UI angle. Basically, it will be very helpful for data debugging, if one can click the Module from Module Analyzer and see 100 example records for that Module. For this use case, it is a SmvModule level function. This could actually implemented in SmvApp. 

Column level meta data is a tricky one. Column description is meaningful at output module. For any intermediate modules or SRDD, in code comments are good for capturing descriptions. However, there could be other use cases that we need to label data fields. For example, some field need to be labeled as demo-info-derived, so that they and their descendants should not be used in some models because of regulations. In that case we clearly need to propagate the label.

## SmvSchema Road Map     

* Rename it!
* Support format info in schema file to better support FRL and also CSV
* Capable to hold column level meta data

## SmvModule Road Map

* Re-version when code change
* Module level meta data and persist in file (runTime, count, edd)

## Other Needs

* Pass through EDD (use accumulator to implement some EDD tasks)
* Schema Discovery to support 1 record attach to Schema file


# Column Meta Data support details
For variables to be shared with multiple teams, the meta information about them need to be shared too. In ideal case, the code should carry those meta data along with data schema. SMV will support variables with meta data through a 2-phase plan.

## Short term solution
Implement meta data within SmvSchema class, and allow user to set it manually. Cleint code is documented in issue #103. No propagation.

## Long term solution

Need to monitor Spark project itself closely to see whether Spark will support this internally.

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

