# FAQ

## What is SMV? 
SMV is a Spark library to enable quickly variable creations for real world data application 
development. The relation to Spark Stack is the following
![Alt text](https://rawgit.com/TresAmigosSD/SMV/master/docs/images/tech_stack.png)

## Who are the users of SMV
SMV could be used for either Data Engineers or Data Scientists.

SMV could help Data Engineers on:
* A very simple modularized framework for data ETL and manipulation
* Similar concepts of data manipulation as table operations and queries in DW
* Software development style code management and more importantly data management
* Obvious variable creation functions and syntax to capture the intend instead
  of implementation details. Easy to share with non-technical people
* SmvModule management with meta data to easily visualize, analyze and
  optimize data flow  

SMV could help Data Scientists on:
* Simple variable creating functions and languages which translate the intend
  or meaning of the variable logic, instead of spend time of figuring out how
  to implement a logic 
* Utilize the power of Spark without very deep knowledge of how Spark core or
  even SparkSQL works
* Interactive Shell integration for quick data discovery 
* Discovery and Development iterations all combined in the same framework for
  quickly automating discovered insights to product ready code

## What is the pre-request of using SMV
Although SMV is a Scala library on top of Spark stack, to use it for basic data discovery 
and data development, the user does not need to know too much on either Spark or Scala. 

SMV document, especially the application framework doc and interactive shell doc, could be 
a good starting point. Most relevant knowledge on Spark is the DataFrame operation session
of the [Spark SQL and DataFrame](http://spark.apache.org/docs/latest/sql-programming-guide.html).  

We are creating a project template for quick start.

## Do I need to setup a cluster to start using SMV?
No. Spark and SMV both works well on local machines. 
Although Spark and SMV only depend on JVM, I am still not sure whether we can setup them on a
Windows machine without using any virtual machine software. Suggest to use Linux or Max OS as
the operating system to run Spark+SMV locally. 
Simply install JDK and Maven, and download a Spark pre-build package, your system is ready for 
using SMV.

## I setup my SMV, what's next?
How to run? How to check the results?

## How to explore?
