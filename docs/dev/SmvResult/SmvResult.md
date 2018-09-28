# Generic Module Results

There are circumstances in which it would be useful for an `SmvModule` to return a result which is not a `DataFrame`. For example, when using SMV for modeling it would be helpful to train the model in one module and score data with the model in another module downstream. In this scenario, we would also like to persist the model as an intermediate result as we would with a `DataFrame`.

We currently handle the "model" special case by converting the model result into a string (by pickle) and stuffing that into a single row `DataFrame`.
This approach will not scale to other types well.  For example, we would like to support "generic" modules that may depend-on and return types such
as Pandas data, H2O data, none-serializable models, etc.

# Solution
The solution is to wrap all module results into a concrete implementation of `SmvResult` class.
`SmvResult`s would be responsible for persisting/reading result data.  The `SmvModule` will no longer assume results/inputs are Spark `DataFrame`.

Each concrete `SmvModule` type would have to specify the corresponding `SmvResult` class that can handle the result of it's `run` method.
The current `SmvModule` can continue to be specific to Spark modules and can be bound to `SmvDataframeResult` result.
Other modules types can specify different result types.
For example, `SmvPandasModule` can specify `SmvPandasResult` as it's result type.
Downstream modules that depend on a instance of `SmvPandasModule` can assume that they will receive a pandas `DataFrame` as input.

## Example client code
```python
class M1_S(SmvModule):
   """This is a spark dataframe module"""
   def run(i):
     return DataFrame(...) # spark dataframe

class M2_P(SmvPandasModule):
   """This is a pandas dataframe module"""
   def run(i):
     return pandas.createDataFrame(...) # pandas dataframe

class M3_J(SmvModule):
   """spark module that combines pandas+spark dataframes"""
   def requiresDS(): return [ M1_S, M1_P ]
   def run(i):
      s = i[M1_S] # this is a spark DataFrame
      p = i[M1_P] # this is a pandas DataFrame

      return s.join(p.toSpark(), ...) # spark dataframe
```

**Note:** user code doesn't know or see any reference to `SmvResult`.
By the time the `run()` method is called, the raw results are extracted from `SmvResult`
