# Testing Your Application

SMV provides a framework to facilitate testing applications. This framework takes care of setting up the application context for each test so you can do things like resolve `SmvDataSets` to `DataFrames` and verify the results.

## Set up

All of your tests should be found in your application's `src/test/python`. The name of every file containing tests should begin with "test" - see SMV's `src/test/python` for examples. Tests should be organized as methods of classes that inherit from `test_support.smvbasetest.SmvBaseTest`. Each test method should also begin with the word "test".

E.g.

```Python
from pyspark.sql.functions import col
from test_support.smvbasetest import SmvBaseTest

class GroupedDataTest(SmvBaseTest):
    def test_smvFillNullWithPrevValue(self):
        df = self.createDF("k:String; t:Integer; v:String", "a,1,;a,2,a;a,3,b;a,4,")
        res = df.smvGroupBy("k").smvFillNullWithPrevValue(col("t").asc())("v")
        expect = self.createDF("k:String; t:Integer; v:String",
              """a,1,;
                 a,2,a;
                 a,3,b;
                 a,4,b"""
        )
        self.should_be_same(expect, res)
```

## Assertions

`SmvBaseTest` inherits from `unittest.TestCase`, so all of `TestCase's` assertion methods are available. Additionally, the 'contents' of a `DataFrame` irrespective of order can be compared using `SmvBaseTest.should_be_same` (see the above example).

## Running Modules

To get the result of a modules, use `SmvBaseTest.df`, much like the `smv-pyshell` method `df`. You can create a `DataFrame` to compare the result to using `SmvBaseTEst.createDF` (see the above example).

## SMV Args

Tests may require special SMV args. SMV args can be specified by overriding the `smvAppInitArgs` method of the test class. The default value is `[-m, None]`. SMV args specified at the command line will be combined with test-specific args for each test class.

## Running tests

Run all of your tests with `smv-pytest`. This will discover and run all of the tests organized as described in [Set Up](#set-up). The name of a test module, test class, or test method can be specified with `-t` to run just that grouping of tests (e.g. `smv-pytest -t testSmvGroupedData.GroupedDataTest`).
