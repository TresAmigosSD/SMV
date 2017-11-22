## SmvModel and SmvModelExec
For modeling with SMV, you may want the result of a module to be a data model and not a DataFrame. A typical `SmvModule` expects a `DataFrame` result, which is easily persisted, but `SmvModel` supports any type of result that is serializable with `pickle`. Train and return your model in the `run` method, and SMV takes care of the rest. To apply the model, use  `SmvModelExec`.

**Python only**
```python
class MyModel(smv.SmvModel):
  def requiresDS(self):
    return [MyInput]

  def run(self, i):
    # train your model here
    return train_model(i[MyInput])

class MyModelExec(smv.SmvModelExec):
  def requiresDS(self):
    return [MyOtherInput]

  def requiresModel(self):
    return MyModel

  def run(self, i, model):
    # score your data here. the `model` argument is the result of MyModel
    return model.score(i[MyOtherInput])
```

From `smv-pyshell`, you can see the result of your `SmvModel` with `getModel`:

```python
>>> m = getModel('MyModelExec')
>>> m.show()
```
