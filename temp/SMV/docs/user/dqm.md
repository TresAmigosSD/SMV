# Validation & Data Quality Management (DQM)

Both `SmvFile` and `SmvModule` has a "Validation" mechanism. SmvApp will
automatically validate the result `DataFrame`.

If the validation result failed, the process will be terminated.

Currently, there are 2 types of validations implemented,
* Parsing validations
* Data Quality Management validations

## Parsing Validation
When we load a Csv file, there are always potential parsing
issues.

Typically 2 types of issues,
* Number of fields in records doesn't match the number of fields specified in schema file
* Some fields of some records do not match schema

For either of the two cases, the record which caused the problem will be rejected. The question
is that whether we fail the entire `SmvFile` and terminate. Different problems might need to handle
this differently.
Also even if we want to fail the entire `SmvFile`, we should rather log more than one "bad" records
for easy debugging.

The default behavior of `SmvCsvFile` or any other input modules with Csv parser will fail the DF 
reading operation when the first parsing error happens, and will throw an exception. This behavior 
can be changed by override the `failAtParsingError` and `dqm` methods

By default, any parser error will throw exception. This behavior is controlled
by the `failAtParsingError` attribute of `SmvCsvFile`. The default value is `True`. To change that we
can override it

```python
class Myfile(smv.SmvCsvFile):
    def path(self):
        return "accounts/acct_demo.csv"
    def failAtParsingError(self):
        return False
```

With above setting, the `SmvCsvFile` will simply persist the validation result and keep moving.

Either terminating the process or not, as long as the log is nontrivial, it will be logged with level "warning".
The result will also by persisted in the `SmvModule` persisted metadata with postfix `.meta`.

Sometimes we need more flexibility on specifying the terminate criteria. For example, I can tolerate
less than 10 parser errors, if more than that, terminate. Here is an example of how to specify that,

```python
import smv.dqm
...
class Myfile(smv.SmvCsvFile):
    def path(self):
        return "accounts/acct_demo.csv"
    def failAtParsingError(self):
        return False
    def dqm(self):
        return dqm.SmvDQM().add(dqm.FailParserCountPolicy(10))
```

Please refer the `DQMPolicy` session below.

The logged dqm result has:

* The total number of rejected records, and
* Some example rejected records

**Note** that we only log a limited number of rejected records to prevent run-away rejections.

An example parsing validation log is like the following

```json
{
  "passed":false,
  "errorMessages": [
    {"ParserError":"Totally 5 records get rejected"}
  ],
  "checkLog": [
    "java.text.ParseException: Unparseable date: \"130109130619\" @RECORD: 123,12.50  ,130109130619,12102012",
    "java.text.ParseException: Unparseable date: \"109130619\" @RECORD: 123,12.50  ,109130619,12102012",
    "java.text.ParseException: Unparseable date: \"201309130619\" @RECORD: 123,12.50  ,201309130619,12102012",
    "java.lang.IllegalArgumentException: requirement failed @RECORD: 123,12.50  ,12102012",
    "java.lang.NumberFormatException: For input string: \"001x\" @RECORD: 123,001x  ,20130109130619,12102012"
  ]
}
```


## DQM

Although the parser enforced data schema, there are typically more things need to be checked on
real-world data. Here are some examples,

* `Age` should be between 0 and 120
* `Gender` should only have 3 values `m`, `f`, and `o`
* `Price` should be between 0.01 and 1,000,000.00

Record by record, above rules could be checked, and depend on the need, they can be fixed.
The `SmvDQM` framework provides `Rule`s and `Fix`es to address them.

### DQMRule & DQMFix

Since `dqm` is a sub-package, to use it one need to
```python
import smv.dqm
```

Since both `SmvFile` and `SmvModule` provide a `dqm` method to define the rules, one can override
it to add rules.

```python
class MyModule(smv.SmvModule):
    """example module with dqm"""
    ...
    def run(self, i):
      ...
    def dqm(self) = return smv.SmvDQM().add(
        dqm.DQMRule(F.col("Price") < 1000000.0, "rule1", dqm.FailAny())
    ).add(
        dqm.DQMFix(F.col("age") > 120, F.lit(120) as "age", "fix1")
    )
```

Each `DQMRule` or `DQMFix` takes a `name` and `DQMTaskPolicy` parameter in addition to the logic. The following
`DQMTaskPolicy`s can be chosen from
* `FailNone` - this rule or fix will not trigger Validation fails by itself
* `FailAny` - whenever this rule or fix be triggered, fail the validation
* `FailCount(n)` - will fail the validation if the rule or fix was triggered >= n times
* `FailPercent(r)` - will fail the validation if the rule or fix was triggered >= r * total record. (r is in between of 0, 1)

If not specified, the default `DQMTaskPolicy` is `FailNone`.

Other than the generic `DQMRule` and `DQMFix` interface, SMV provides some build-in rules and fixes.
* `BoundRule(col: Column, lower: T, upper: T)` - lower <= col < upper
* `SetRule(col: Column, set: Set[Any])` - col in set
* `FormatRule(col: Column, fmt: String)` - col matches fmt
* `SetFix(col: Column, set: Set[Any], default: Any)` - "default" if "col not in set"
* `FormatFix(col: Column, fmt: String, default: Any)` - "default" if "col not match fmt"

Please note that there is no `BoundFix`, since the upper bound and lower bound should have
separated `Fix`es.

The rules and fixes defined in the `dqm` method will be applied to the result `DataFrame` of the `run` method.

### DQMPolicy

A `DQMPolicy` defines a requirement on the entire `DataFrame`. As in above example we can add
a policy

```python
def dqm() = dqm.SmvDQM().add(
    dqm.DQMRule(F.col("Price") < 1000000.0, "rule1")).add(
    dqm.DQMRule(F.col("Price") > 0.0, "rule2")).add(
    dqm.DQMFix(F.col("age") > 120, F.lit(120) as "age", "fix1")).add(
    dqm.FailTotalRuleCountPolicy(100))
```

Here `FailTotalRuleCountPolicy(...)` is a predefined `DQMPolicy`, which check the total count of
all the rules in this `DQM`, if the total count is within the threshold, the validation will
pass, otherwise will fail.

There are 5 build-in `DQMPolicy`s
* `FailParserCountPolicy(n)` - fail when total parser error count >= n
* `FailTotalRuleCountPolicy(n)` - fail when total rule count >= n
* `FailTotalFixCountPolicy(n)` - fail when total fix count >= n
* `FailTotalRulePercentPolicy(r)` - fail when total rule count >= total records * r, r in (0,1)
* `FailTotalFixPercentPolicy(r)` - fail when total fix count >= total records * r, r in (0, 1)

One can also create user defined policies.

**Scala**
```scala
val policy: (DataFrame, DQMState) => Boolean = {(df, state) =>
  state.getRuleCount("rule1") + state.getFixCount("fix1") < 1000
}
override def dqm() = SmvDQM().
    add(DQMRule($"Price" < 1000000.0, "rule1")).
    add(DQMRule($"Price" > 0.0, "rule2")).
    add(DQMFix($"age" > 120, lit(120) as "age", "fix1")).
    add(DQMPolicy(policy, "my_udp1"))
```

**Python**
```python
Not implemented yet
```

### DQMState

`DQMState` provides the following methods to access its contents, one can use them to build policies.
* `getRecCount(): Long` - total number of records in this DF
* `getFixCount(name: String): Int`  -  for the fix with the given name, return the time it is triggered
* `getRuleCount(name: String): Int` - for the rule with the given name, return the time it is triggered
* `getTaskCount(name: String): Int` - for the rule or fix with the given name, return the time it is triggered
* `getRuleLog(name: String): Seq[String]` - for the rule with the given name, return the example failed records
* `getAllLog(): Seq[String]` - return all the example failed records from all the rules
* `getTotalFixCount(): Int` - return the total number of times fixes get triggered
* `getTotalRuleCount(): Int` - return the total number of times rules get triggered
