# Setups

You need to set the Maven memory to avoid compile failure  
```shell
export MAVEN_OPTS="-Xmx512m -XX:MaxPermSize=128m"
```

# Code Style

## Column class

Since 1.3, Catalyst Expression is hidden from final user. A new class ```Column``` is created as a user interface. 
Internally, it is a wrapper around Expression. 

Before we use 2 types of Expressions in ```select```, ```groupBy``` etc., one is a Symbol, which refers to an original 
column of the Srdd, the other is a real Expression like ```Sqrt('a)```. When it was Symbol, with ```import sqlContext._```,
the Symbol is implicitly converted to analysis.UnresolvedAttribut, which is an Expression.

Since 1.3, an implicit conversion from Symbol to Column is provided by ```import sqlContext.implicits._```. However, 
both example code and convenience alternative methods are more toward using ```String``` to represent Column names instead 
of using Symbol. 
Also it specifically defined the "Symbol" or "String" represented column names as a class ```ColumnName``` which is a
subclass of ```Column```. 

Here are some examples:

Even without ```import sqlContext.implicits._```, you can do
```scala
df.select("field1")
```
The same thing can be written as
```scala
df.select(df("field1"))
```
Where ```df(...)``` literally look for the column withing ```df``` with name "field1" and return a ```Column```. 
You can also do
```scala
import df.sqlContext.implicts._
df.select($"field1")
```
The magic here is that there is an implicit class in ```sqlContext.implicits```, which extends 
[StringContext](http://www.scala-lang.org/files/archive/nightly/docs/library/index.html#scala.StringContext)

The problem is when to use which way to do things.

### Use String directly 
Although the original ```select``` taking ```Column```'s
```scala
def select(c: Column*)
```

One can use String in ```select``` directly, because ```select``` has a convenience alternative method which defined as
```scala
def select(s1: String, others: String*) 
```

As you can see to avoid ambiguity, instead of define ```select(s: String*)```, the String version actually split the 
parameter list. It provide the convenience as call it with explicit string parameters, it also caused a problem that we 
cant do this 
```scala
val keys=Seq("a", "b")
df.select(keys: _*) //will not work!
```

### Explicitly specify the dataframe for a column
```
df("a")
```
Is actually ```DataFrame.apply("a")```, literally search for column "a" in Dataframe "df". However sometimes we want to construct some general ```Column``` without
specifying any Dataframe. We need the next method to do so.

### General Column from String
```
$"a"
```
will create a general Column with name "a". Then how we use variables in this weird syntax? 

Actually ```$``` here is a method on ```StringContext```. An equivalent is ```s``` in ```s"dollar=${d}"```. With this analogy, 
we can figure out the way to use variables 
```
val name = "a"
$"$name"
```

Finally we have a way to select a List of fields
```scala
val keys=Seq("a", "b").map(l=>$"$l")
df.select(keys: _*)
```
 
### Symbol still works through implicit conversion
```scala
df.select('single * 2) 
```
will still work. 

However since it was through implicit conversion, the following doesn't work just like the string case:
```scala
val keys=Seq('a, 'b)
df.select(keys: _*) //doesn't work
``` 

You need to do
```scala
df.select(keys.map(l=>$"${l.name}"): _*)
```

### What "as" do

Here is what we do in the past to define new variables:
```scala
df.select('a * 2 as 'b)
```
It will still work in 1.3 as long as you ```import df.sqlContext.implicits._```. 

Here is what happened:

* The ```'a``` implicitly converted to a ColumnName (which is a Column)
* ```*``` is a method of ```Column``` which take 2 (as Any) as parameter 
* ```as``` is another method of ```Column```, which can take String or Symbol, 
```'b```, as parameter and return a ```Column```.

So basically if we consider ```as``` as an operator, left of it should be a ```Column``` 
and right of it should be either a ```Symbol``` or a ```String```.

To follow the 1.3 document examples, using String instead of Symbol to represent 
column names:
```scala
df.select($"a" * 2 as "b")
```

### No Expressions for the end users

As adding the ```Column``` class and refined the ```DataFrame``` interface, the end user interface is focused 
on only 2 concepts: 

* The DataFrame to hold the data, and
* The Column defines the element within a DataFrame
  
And

* ```select```, ```groupBy```, ```agg```, etc. are DataFrame methods.
* ```as```, ```+```, ```*```, ```substr```, ```in```, etc. are Column methods.

Other "Expressions" are interfaced through ```org.appache.spark.sql.functions```, such as

* ```avg```, ```sum```, ```abs```, ```sqrt```, etc.

For SMV, we should follow the same principle and provide interfaces through extending those 3 groups:

* DataFrameHelper (now SchemaRDDHelper) to extend DataFrame methods
* ColumnHelper (some of the nonaggregate functions now) to extend Column methods 
* package (or explicitly org.tresamigos.smv.functions._) to extend sql.functions

## Changes on ```groupBy```

Before 1.3, we do group-by aggregation as
```scala
srdd.groupBy('key1, 'key2)(Sum('v1) as 'sv1, Count('v2) as 'cv2)
```

One of the biggest change of 1.3 is to separate the 2 functions of ```groupBy```. Now the way we should do above is
```scala
df.groupBy('key1, 'key2).agg(sum('v1) as 'sv1, count('v2) as 'cv2)
```

Or using String for column names
```scala
df.groupBy("key1", "key2").agg(sum("v1") as "sv1", count("v2") "cv2")
```

Although when we using Symbol, the change looks minimal, behind the sign the change is fundamental. In 1.3, ```groupBy```
as DataFrame method, returns a ```GroupedData``` object, and ```agg``` is a method of ```GroupedData``` class. Although 
there are not too many methods supported by ```GroupedData``` yet, the introducing of the ```GroupedData``` class opens
a door of much more flexible DataFrame processing. 

Let's review our need of the ```smvChunkBy``` method of SchemaRDD in current SMV. It's basically create a ```GroupedData```
object and expose it to Scala as iterators to process. Within the new framework, we can simply create a ```GroupedDataHelper``` 
class and implicitly convert ```GroupedData```. Through the helper, we can add methods fulfill the requirements of 
```smvChunkBy```. Though this way, we don't need to go back to RDD and do what we need in DF end-to-end.

We can even extend ```GroupedData``` method to return a ```GroupedData``` method to chain things together.   


## Column methods/operators and function

One big benefit of wrapping Expression with Column is that we can define methods/operators on Column class, 
which make Column more like a Scala data type instead of a totally different DSL. 

So now 

* ```+```, ```-```, ```*```, ```/```
* ```<```, ```>```, etc.
* ```&&```, ```||```, etc.

are all Column methods/operators as other Scala classes which supporting arithmetics.

Also some string operations are supported, such as ```substr```

Those methods are all defined in [Column class](http://spark.apache.org/docs/latest/api/scala/index.html#org.apache.spark.sql.Column)

Another class operate on Column's is [sql.functions](http://spark.apache.org/docs/latest/api/scala/index.html#org.apache.spark.sql.functions$)

Basically there are 3 types of functions:

* Aggregate functions
* NonAggregate functions
* UDF

The Aggregate functions are pretty much wrappers of Aggregate Expressions. 
All the NonAggregate functions could be implemented as methods. Some of them are mainly used by people
as functions instead of method, for example we typically do ```sqrt($"a")``` instead of ```$"a".sqrt```. 
Some of them act on multiple Columns, such as ```coalesce```. 
However, there are still some I don't know why they should be functions instead of method of Column, such as
```lower``` and ```upper```.

```udf``` is really handy if you are sure that the Column you are dealing with has no nulls. The issue is that
although udf simply wrapped around ```ScalaUdf```, it determines the ```DataType``` from the Scala function,
which passed to it. This caused a problem that I can't define a function which returns ```Double``` and make it ```nullable```, 
since Scala will not allow null be a ```Double```. 

Instead, the right way to do it is to define the function to return ```java.lang.Double```, which actually allows 
```null``` value. Here is an example which convert string like ```12,345``` to a double ```12345.0```.

```scala
    val nf = java.text.NumberFormat.getNumberInstance(java.util.Locale.US)
    val s2d: String => java.lang.Double = { s => 
      if (s == null) null
      else nf.parse(s).doubleValue()
    }
```

Within Scala there are actually 3 types for double: ```Double``` is a Scala class, ```java.lang.Double``` is a java class, and
```double``` is a JVM type. Internally a Scala ```Double``` can be implicitly converted to either the a boxed java ```java.lang.Double``` 
object, or directly to ```double``` as the primitive data type. It is clear the since ```double``` is a data type, it can't be ```null```.
Since Scala ```Double``` need to convert to both, it can't be ```null``` either. Scala will generate error if you try to pass ```null``` to 
a ```Double```. However, the boxed ```java.lang.Double``` is a normal java object, ```null``` IS a valid value. 

# To be converted

* DQM -- low priority, should be redesigned with SmvApp logging