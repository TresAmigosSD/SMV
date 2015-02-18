# SmvApp Framework

The SmvApp is a very simple data application framework to support:

* No overhead of developers to use RDD and SchemaRDD to manipulate data and create new data set and variables
* Ability to use the same framework for quickly creating ad-hoc analysis and the ad-hoc queries should be able to easily transform to production package
* Easy testing of module levels. Developer shouldn't have to create many repetitive and cumbersome scaffolding to test a module
* Intermediate data persisting and versioning in debug mode. Data versioning is critical for applying test driven development on data application development
* Ability to test/run at the module level and at the application level. Developer should be able to test run modules they currently working on
* Same framework should also allow the creation of execution graphs at the application level. 
* Provides utility to managing the end to end dependency graph, persisted data meta info and execution logging

## Design

The SmvApp has 3 components: ```SmvApp``` class, ```SmvFile``` and ```SmvModule```. Both ```SmvFile``` and ```SmvModule``` classes extends an abstract class ```SmvDataSet```. 
The core concept of the framework is the ```SmvModule```. ```SmvModule``` is an higher level data representation than SchemaRDD. It basically defines a dataflow block, which has multiple data souses (SchenaRDD) in and ONE data out (SchemaRDD). 

Design Diagram:
![Alt text](https://rawgit.com/TresAmigosSD/SMV/master/docs/appFramework.files/appFramework.svg)

## Define Your First App/Module

### Create an SmvAPP

Here is and example of a full SmvApp (copied from [MVD](https://github.com/TresAmigosSD/MVD))
```scala
class CmsApp(_appName: String,
                  _args: Seq[String],
                  _sc: Option[SparkContext] = None
                  )
      extends SmvApp(_appName, _args, _sc) {

  val num_partitions = sys.env.getOrElse("CMS_PARTITIONS", "32")
  sqlContext.setConf("spark.sql.shuffle.partitions", num_partitions)

  override def getModulePackages() = Seq(
    "org.tresamigos.mvd.projectcms.adhoc",
    "org.tresamigos.mvd.projectcms.phase1",
    "org.tresamigos.mvd.projectcms.phase2"
  )
}

object CmsApp {
  val ca = CsvAttributes.defaultCsvWithHeader
  val caTsv = CsvAttributes.defaultTsvWithHeader

  // The file path is relative to $DATA_DIR as a environment variable
  val cms_raw_no_2nd_line =  SmvFile("cms_raw_no_2nd_line", "input/cms_raw_no_2nd_line.csv", caTsv)

  def main(args: Array[String]) {
    new CmsApp("CmsApp", args).run()
  }
}
```

For each project, we typically have one class extends SmvApp instead of just create a singleton. The reason is for more flexible uni-testing on individual modules. Within the class definition, app specific spark configuration can be set. As in this example, we take an environment variable ```CMS_PARTITIONS``` to override the default number of partitions, 32. It also override the ```getModulePackages``` methods to create the project scope. As you can see, there could be multiple JAVA/Scala packages in an App.

The companion object provides an entry to the app, and also define all the SmvFiles as variables for modules to use. 

### Create an Module
```scala
object CmsRaw extends SmvModule("Load cms raw data") {

  override def requiresDS() = Seq(
    CmsApp.cms_raw_no_2nd_line
  )

  override def run(i: runParams) = {
    val srdd = i(CmsApp.cms_raw_no_2nd_line)
    srdd
  }
}
```
Here is a simple SmvModule which reads in the SmvFile as defined in the CmsApp and simply passes it down. In real applications, there could be some simple ETL tasks performed here. 

Here is an example of a downstream module which consume ```CmsRaw```:

```scala
object Ex01SimpleAggregate extends SmvModule("Example 01: to demo simple use") {

  override def requiresDS() = Seq(
    CmsRaw
  )

  override def run(i: runParams) = {
    val srdd = i(CmsRaw)
    import srdd.sqlContext._

    srdd.groupBy('npi)(
      'npi,
      Sum('line_srvc_cnt) as 'total_srvc
    )
  }
}
```

This example simply aggregate one variable on ```'npi```. As you can see the minimal work for defining a module is to override 2 methods: ```requiresDS``` and ```run```. Within ```run``` method, the upstream modules or files can be accessed from the parameter, ```i: runParams``` passed into it. 

As building blocks, SmvModule will the unit developers will work on every day. 

## Run 

After a project is compiled, it can be submit to Spark to run. With the SmvApp framework, you can specify which module you want to generate. Also, you can simply generate the dependency graph without really run the code. Here is the example submit command for the CmsApp:
```shell
DATA_DIR=./data/cms spark-submit --master local[2] --class org.tresamigos.mvd.projectcms.core.CmsApp target/mvd-1.0-SNAPSHOT-jar-with-dependencies.jar -d org.tresamigos.mvd.projectcms.adhoc.Ex01SimpleAggregate
```
This command generate the Ex01SimpleAggregate output SchemaRDD under debug mode (-d). With debug mode, all the intermediate results are persisted as CSV files also (CmsRaw here). If you find something wrong on Ex01SimpleAggregate, you can just go and fix the code and re-run above command. In that case, the ```CmsRaw``` will not be re-calculated.

When an app grows, there could be one hundred modules or more. In that case, if you find some error in the middle, it could be hard to figure out all the downstream modules and regenerate all of them. In this case, the ```version``` method of SmvModule becomes very handy. In above example, assume we found we need to add some filter in the ```CmsRaw``` module. Instead of our simply app, which just has 1 module depends on ```CmsRaw```, there could be 20 modules depend on ```CmsRaw``` in a real project. To make it work, we can simply add the filter in the ```CmsRaw``` module, and add override the version method as

```scala
override def version() = 1
```

Since the default value of version is 0, above line changes the version number of ```CmsRaw```, and more importantly, since the persisted data file's version is actually the sum of the version numbers of SmvModule's it depends on, the App will know that all the existing persisted files which depend on ```CmsRaw``` are out of date and need to be re-generated. So you simply need to re-run the submit command on the most downstream Module and all the modules depend on ```CmsRaw``` will be updated.

As a bonus, the same SmvApp submit command with a parameter ```-g``` instead of ```-d``` will create a ```dot``` file to show the decency graph of the specified module. You can use Graphviz ```dot``` command to convert the graph to either ```png``` or ```svg``` file. For the example above, a [dot file](appFramework.files/Ex01SimpleAggregate.dot) will be created, and the png version:
![Alt text](https://rawgit.com/TresAmigosSD/SMV/master/docs/appFramework.files/Ex01SimpleAggregate.png)

