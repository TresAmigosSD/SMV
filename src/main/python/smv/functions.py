#
# This file is licensed under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from smv import SmvApp
from pyspark.sql.column import Column
from pyspark.sql.functions import udf
from utils import smv_copy_array

def nGram2(c1, c2):
    """2-gram UDF with formula (number of overlaped gramCnt)/max(c1.gramCnt, c2.gramCnt)

        Args:
            c1 (Column): first column
            c2 (Column): second column

        Returns:
            (Column): 2-gram
    """
    return Column(SmvApp.getInstance()._jvm.org.tresamigos.smv.smvfuncs.nGram2(c1._jc, c2._jc))

def nGram3(c1, c2):
    """3-gram UDF with formula (number of overlaped gramCnt)/max(s1.gramCnt, s2.gramCnt)

        Args:
            c1 (Column): first column
            c2 (Column): second column

        Returns:
            (Column): 3-gram
    """
    return Column(SmvApp.getInstance()._jvm.org.tresamigos.smv.smvfuncs.nGram3(c1._jc, c2._jc))

def diceSorensen(c1, c2):
    """2-gram UDF with formula (2 * number of overlaped gramCnt)/(s1.gramCnt + s2.gramCnt)

        Args:
            c1 (Column): first column
            c2 (Column): second column

        Returns:
            (Column): 2-gram
    """
    return Column(SmvApp.getInstance()._jvm.org.tresamigos.smv.smvfuncs.diceSorensen(c1._jc, c2._jc))

def normlevenshtein(c1, c2):
    """Levenshtein edit distance metric UDF

        Args:
            c1 (Column): first column
            c2 (Column): second column

        Returns:
            (Column): distances
    """
    return Column(SmvApp.getInstance()._jvm.org.tresamigos.smv.smvfuncs.normlevenshtein(c1._jc, c2._jc))

def jaroWinkler(c1, c2):
    """Jaro-Winkler edit distance metric UDF

        Args:
            c1 (Column): first column
            c2 (Column): second column

        Returns:
            (Column): distances
    """
    return Column(SmvApp.getInstance()._jvm.org.tresamigos.smv.smvfuncs.jaroWinkler(c1._jc, c2._jc))

def smvFirst(c, nonNull = False):
    """Variation of Spark "first" which also returns null values

        Since Spark "first" will return the first non-null value, we have to
        create our version smvFirst which to retune the real first value, even
        if it's null. Alternatively can return the first non-null value.

        Args:
            c (Column: column to extract first value from
            nonNull (bool): If false, return first value even if null.
                            If true, return first non-null value.
                            Defaults to false.

        Returns:
            (object): first value
    """
    return Column(SmvApp.getInstance()._jvm.org.tresamigos.smv.smvfuncs.smvFirst(c._jc, nonNull))

def smvCreateLookUp(m, default, outputType):
    """Return a Python UDF which will perform a dictionary lookup on a column

        Args:
            m (dictionary): a Python dictionary to be applied
            default (any): default value if dictionary lookup failed
            outputType (DataType): output value's data type

        Returns:
            (udf): an udf which can apply to a column and apply the lookup
    """
    return udf(lambda k: m.get(k, default), outputType)

def smvArrayCat(sep, col):
    """For an array typed column, concat the elements to a string with the given separater.

       Args:
            sep: a Python string to separate the fields
            col: a Column with ArrayType

       Return:
            (col): a Column in StringType with array elements concatenated
    """
    return Column(SmvApp.getInstance()._jvm.org.tresamigos.smv.smvfuncs.smvArrayCat(sep, col._jc))

def smvCollectSet(col, datatype):
    """An aggregate function, which will collect all the values of the given column and create a set as an array typed column.
       Since Spark 1.6, a spark function collect_set was introduced, so as migrate to Spark 1.6 and later, this smvCollectSet
       will be depricated.

       Args:
            col (Column): column to be aggregated on
            datatype (DataType): datatype of the input column
    """
    return Column(SmvApp.getInstance()._jvm.org.tresamigos.smv.python.SmvPythonHelper.smvCollectSet(col._jc, datatype.json()))

def smvStrCat(head, *others):
    """Concatenate multiple columns to a single string. Similar to `concat` and `concat_ws` functions in Spark but behaves differently
       when some columns are nulls. 
    """
    if (isinstance(head, basestring)):
        sep = head
        cols = list(others)
    elif (isinstance(head, Column)):
        sep = ""
        cols = [head] + list(others)
    else:
        raise RuntimeError("first parameter must be either a String or a Column")
    app = SmvApp.getInstance()
    return Column(app._jvm.org.tresamigos.smv.python.SmvPythonHelper.smvStrCat(sep, smv_copy_array(app.sc, *cols)))
