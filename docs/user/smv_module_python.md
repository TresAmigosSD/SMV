# SMV Python Modules

# SmvPyDataSet

The Python base class `SmvPyDataSet` implements shared functionalities such as `modulePath()`, `fqn()`, `isInput()` etc.  It also ensures that all subclass must implement the following abstract methods:
* `description` - what is this dataset about
* `requiresDS`  - the upstream dependencies, in other words, what other datasets need to be run first
* `run(i)`      - how to compute this dataset from the input declared in `requiresDS`

The `description` method is used instead of making it part of the constructor (as in Scala) so that the complex syntax for subclass constructor can be avoided.  This pattern is applied consistently throughout the SMV Python API.

# SmvPyCsvFile

The simplest type of input dataset.  To create a Python module representing input from a csv file, subclass `SmvPyCsvFile` as show below
```python
from smv import *
class PythonCsvFile(SmvPyCsvFile):
    def path(self):
        return "path/to/filename.csv"

    def csvAttr(self):
        return self.defaultCsvWithHeader()
```
The `path` method (annotated as a property) is equivalent to the constructor parameter `path` in Scala; it returns the path to the input file.  All subclasses must provide an implementation for this method.  The `csvAttr()` method is optional.  The default implementation returns `None`, which maps to `null` in Java.  The meaning of having no `csvAttr()` is such that SMV runtime will read the schema information stored in the corresponding `.schema` file.  Alternatively, the subclass can also provide a csvAttr.  There are 3 factory methods in `SmvPyCsvFile`: `defaultCsvWithHeader`, `defaultTsv` (tab-delimited format), and `defaultTsvWithHeader`.  These correspond to their Scala counterparts.

# SmvPyModule

To create an SMV module in Python, subclass `SmvPyModule` and override the 3 abstract methods listed in the section on SmvPyDataSet.

The `requiresDS` method may return an empty array, if the module does not depend on anything.  The class literal of the dependencies are returned in the array.

The parameter `i` that's passed to the `run` method is named the same way as in the Scala API.  Even though it does take up an often-used name in the scope, we left it as is for the sake of consistency.  The `i` is a dictionary mapping module class to its resulting `DataFrame`s.  Through it modules can retrieve the results of its dependent modules.

Within the `run` methods, all operations on Spark DataFrames are available, as well as most of SMV enrichments to the Spark SQL API.

# SmvPyHiveTable

:soon:
