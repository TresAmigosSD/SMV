# Spark Modularized View (SMV)

## Provide a framework to use Spark to develop large scale data applications

Most use cases that Spark supports out of the box are quick ad hoc data analysis. In those use cases,
Spark is more like a powerful scripting tool for data discovery. SMV is an effort to use Spark
to develop high quality large scale data applications.

The basic working unit within the SMV framework is SmvModule. Here is an example,

```scala
  object StageEmpCategory extends SmvModule("Employment By Stage with Category") {
    override def requiresDS() = Seq(input.EmploymentStateLink);
    override def run(i: runParams) = {
      val df = i(input.EmploymentStateLink)
      import df.sqlContext.implicits._

      df.selectPlus(
        $"EMP" >  lit(1000000) as "cat_high_emp"
      )
    }
  }
```

Data App developers will work module by module. They basically only need to answer
* Who do I depend on?
* What should I generate from the data I depend on


## Synchronize between code management and data management

One key issue of data app development is how to perform version control with data. To address that
SMV binds the data version to the code which generated it.

For example, here is a persisted output file of a SmvModule:

```
com.mycompany.MyApp.stage2.StageEmpCategory_b36cb1b0.csv
                                            ________
                                               |
                                               |
                                      the HashOfHash of the SmvModule
```

For each SmvModule, the `HashOfHash` is created based on
* CRC of current SmvModule's code
* HashOfHash of the SmvModules, which current module depends on
* For input data files, the HashOfHash depends on file full path and modification time

When an SMV app is run, it will automatically recognize changes in the code and re-generate any
impacted data file, and leave all others untouched.


## Data API for large projects/large teams

SMV organizes project as stages. Each stage independently define input modules and output modules.

```
  Stage1
    |- Module1                      Stage2
    |- Module2 as SmvOutput   ---->   |- InputFromS1M2
                                      |- Module2 as SmvOutput
```

In above example, between Stage1 and Stage2, a data interface is defined (InputFromS1M2 is a link
to Module2). When Stage2 require stability of the interface data, Stage1 team can **Publish** a
version of Module2, and Stage2 can depend on the published version. In the mean time, Stage1 team
can still work on Stage1.


## Programmatic data quality management

Each SmvModule can define a dqm (data quality management) method, which will be checked automatically
whenever the module runs. Here is an example,

```scala
  override def dqm() = SmvDQM().
      add(DQMRule($"Price" < 1000000.0, "rule1")).
      add(DQMRule($"Price" > 0.0, "rule2")).
      add(DQMFix($"age" > 120, lit(120) as "age", "fix1")).
      add(FailTotalRuleCountPolicy(100))
```

As a project progresses, the domain knowledge of some given data also gets accumulated, the dqm function
simplifies the effort of including data knowledge into code and enforcing checks against the accumulated knowledge.
