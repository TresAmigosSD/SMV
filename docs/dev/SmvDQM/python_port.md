From issue #997

## DQM API
The entry point to the **DQM** API is the `SmvDQM` builder, which offers these 3 key methods
* add (DQMRule)
* add (DQMFix)
* add (DQMPolicy)

Both `DQMRule` and `DQMFix` are subclasses of `DQMTaskPolicy`, a sealed abstract class that provides wrapper object and classes around pre-defined `DQMPolicy` classes.

A `DQMPolicy` has a `(DataFrame, DQMState) => Boolean` method that determines whether or not the data frame is acceptable.

## What we have right now
(as of Mon Oct  2 16:07:21 PDT 2017)
On the Python side, the user has access to the following 3 classes:
* SmvDQM builder
* DQMRule
* DQMFix

The user also can access the named `DQMTaskPolicy`s such as `FailPercent`,`FailCount`, `FailTotalRuleCountPolicy`, etc.

## Resolution
The above should be sufficient for general use.

If the user would like to write user-defined policies in Python, it should be relatively straight-forward to provide this capability through the following:
* add a method in SmvDQM, such as `add (IDqmPolicy)`, where the `IDqmPolicy` is a Java interface, which can be implemented by a Python class
* create a Python class that would take a Python function and implement that `IDqmPolicy` Java interface with calling that function.

But for now, as this particular use case is not particularly pressing, we will leave the ticket as unfixed.

