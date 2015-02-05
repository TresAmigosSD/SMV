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

# Data Themes and Directional Requirements to SMV

## The Time Dimension
There are 3 types of data regarding to how the time dimension is represented
* **Snapshot Data**: the data are only available on a single timestamp 
* **Panel Data**: Data are measured on a rescheduled period (daily, monthly, etc.)
* **Event Data**: Data are stamped at the time when the event happen

From business angle there are typically only 1 time dimension for most of the problems. However, multiple time dimension is very typical for hospitality business. 
From technical angle it is very common to have multiple time dimensions. For example, time of event, time of the record captured in DW, time of the record be extracted from DW, etc. However, since we assume SMV will be main used for business application development, the functions around time should be mainly focus on the business time dimension, instead of the technical dimension, which should be mainly for data integrity at ETL step.

Regarding the output time dimension, most BI like reporting will prefer Panel Data format, while recommendation system or similar front end decision support will need event data format.

## Entity Category Structures
There are typically 2 type of entity category structures
* **Hierarchical**: Examples: product category hierarchy, geo unit hierarchy, etc. 
* **Features**: Examples: topics of a book, themes of a Ad, etc.

Typical business problem will have both those 2 types and each of them could have multiple cases. For example, for measuring product performance of a CPG company, there are 3 major hierarchies, product hierarchy, operation unit (stores) hierarchy, and geo market hierarchy 


