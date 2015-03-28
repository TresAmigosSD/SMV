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
of using Symbol. Eg. even without ```import sqlContext.implicits._```, you can do
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
literally search for column "a" in Dataframe "df". However sometimes we want to construct some general ```Column``` without
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
 
 
