# Generic Module Results

There are circumstances in which it would be useful for an `SmvModule` to return a result which is not a `DataFrame`. For example, when using SMV for modeling it would be helpful to train the model in one module and score data with the model in another module downstream. In this scenario, we would also like to persist the model as an intermediate result as we would with a `DataFrame`.

## Pickle & persist

If possible, we would like to use the same mechanisms for persisting these generic results as we do for persisting `DataFrames`. We can accomplish this by serializing the user's result and saving the serialization as a single row `DataFrame`. Since this feature is only needed for Python `SmvModules`, we can use `pickle` for the serialization. Of course, this requires that the result is safely picklable.

## SmvModuleWithObjRes

When the result is read back as an input, we need some indication that the `DataFrame` contains a pickled object. Modules that return generic results will inherit from `SmvModuleWithObjRes` (name will probably be updated upon further consideration).
