# FAQ

## What is SMV?
SMV stands for "Spark Modularized View".
It is a simple data application framework for real world data application
development. The relation to Spark Stack is the following
![Alt text](https://rawgit.com/TresAmigosSD/SMV/master/docs/images/tech_stack.png)

## Who are the users of SMV
SMV could be used by either Data Engineers or Data Scientists.

SMV could help Data Engineers on:
* A very simple modularized framework for data ETL and manipulation
* Similar concepts of data manipulation as table operations and queries in Data Warehouse
* Software development style code management and more importantly data management
* Obvious variable creation functions and syntax to capture the intention instead
  of implementation details. Easy to share with non-technical people
* SmvModule management with meta data to easily visualize, analyze and
  optimize data flow  

SMV could help Data Scientists on:
* Simple variable creating functions and languages which translate the intention
  or meaning of the variable logic, instead of spend time of figuring out how
  to implement a logic
* Utilize the power of Spark without very deep knowledge of how Spark core or
  even SparkSQL works
* Interactive Shell integration for quick data discovery
* Discovery and Development iterations all combined in the same framework for
  quickly automating discovered insights to product ready code

## What is the prerequisite of using SMV
Although SMV is a Scala library on top of Spark stack, to use it for basic data discovery
and data development, the user does not need to know too much on either Spark or Scala.

[SMV User Guide](https://github.com/TresAmigosSD/SMV/blob/master/docs/user/0_user_toc.md),
could be
a good starting point. Most relevant knowledge on Spark is the DataFrame operation session
of the [Spark SQL and DataFrame](http://spark.apache.org/docs/latest/sql-programming-guide.html).

## Do I need to setup a cluster to start using SMV?
No. Spark and SMV both works well on local machines.
Although Spark and SMV only depend on JVM, I am still not sure whether we can setup them on a
Windows machine without using any virtual machine software. Suggest to use Linux or Max OS as
the operating system to run Spark+SMV locally.
Simply install JDK and Maven, and download a Spark pre-build package, your system is ready for
using SMV.

## I setup my SMV, what's next?
Please follow [Getting Start Docs](https://github.com/TresAmigosSD/SMV/blob/master/docs/user/getting_started.md).
It will help you to quickly start using SMV and setup your own project.

Also you can read all the SMV document and try out functions with the example project.

## How can I calculate *?
To create new variables (columns), you need to utilize Column operations. There are 2 places in
Spark you can find them: methods of Column class and functions object under sql package. You can find
all the useful methods and functions on the Scala API doc from Spark.
SMV extends both the methods and functions. The new methods and functions can be find in
[SMV API Docs](http://tresamigossd.github.io/SMV/scaladocs/1.5.2.8/index.html#org.tresamigos.smv.package).

For anything which is not covered above, you can create your own "udf", User Defined Function, through
Scala functions. Here is a simple example

```scala
import org.apache.spark.sql.functions._
val addone = udf({i: Int => i + 1})

val result = df.select($”acct”, addone($“volume”) as “newVolume”)
```
