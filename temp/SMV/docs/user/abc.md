# Abstract Base Classes
In any large projects there will be some datasets that are very similar with only a minor variant.  For example:
* Multiple input datasets that have the same run method but different CSV file paths.
* Modules that group the same data but use a different group value (e.g month / quarter / year).

In keeping with the [DRY](https://en.wikipedia.org/wiki/Don%27t_repeat_yourself) principle, users should avoid code duplication.  An easy way to do that in SMV is to create Abstract Base Classes (ABC) with the common code.

## Extracting the variant
The first step to creating the ABC is extracting the variant.  In the examples above, the variants are the file paths and grouping values.  Because SMV needs to instantiate modules using the default constructor, we shall use abstract methods to provide the variant values in the concrete classes rather than use constructor injection.  For example:
```python
# WRONG WAY TO INJECT VARIANT
class MyModule(smv.SmvModule):
  def __init__(self, file_path):
    self.file_path = file_path

  ... use self.file_path here ...
```
instead, user should use abstract methods:
```python
# CORRECT WAY TO INJECT VARIANT
class MyModule(smv.SmvModule):
  def get_path(self):
    """concrete classes should provide path by overriding this method"""
    pass

  ... use self.get_path() here ...
```

## Full Example
In the example below, the `EmploymentByState` class in the example app created using `smv-init` is modified to become an Abstract Base Class (ABC) with two concrete implementations.

The concrete implementations below only differ in their output column name.  In a real world example, the difference can be any value used by the `requiresDS` and `run` methods.

```python
# Abstract Base Class (ABC) that implements the employment by state module.
class EmploymentByStateBase(smv.SmvModule):
    """Python ETL Example: employ by state"""

    # derived classes should override this method to provide the column name.
    def getOutputColumnName(self): pass

    def requiresDS(self):
        return [Employment]

    def run(self, i):
        df = i[Employment]
        return df.groupBy(F.col("ST")).agg(F.sum(F.col("EMP")).alias(self.getOutputColumnName()))

class EmploymentByStateX(EmploymentByStateBase, smv.SmvOutput):
    # provide the variant here.
    def getOutputColumnName(self): return "EMPX"

class EmploymentByStateY(EmploymentByStateBase, SmvOutput):
    def getOutputColumnName(self): return "EMPY"
```
