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

from pyspark.sql.column import Column
from pyspark.sql.functions import udf

from smv.smvapp import SmvApp
from smv.utils import smv_copy_array, is_string

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
            nonNull (bool): If false, return first value even if null. If true, return first non-null value. Defaults to false.

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
       The Spark version will return null if any of the inputs is null.
       smvStrCat will return null if all of the inputs are nulls, otherwise it will coalesce null cols to blank.

       This function can take 2 forms:
       - smvStrCat(sep, col1, col2, ...)
       - smvStrCat(col1, col2, ...)

       Args:
           sep (String): separater for the concats
           col. (Column): columns to be concatenated

       Return:
           (col): a StringType column
    """
    if is_string(head):
        sep = head
        cols = list(others)
    elif isinstance(head, Column):
        sep = ""
        cols = [head] + list(others)
    else:
        raise RuntimeError("first parameter must be either a String or a Column")
    app = SmvApp.getInstance()
    return Column(app._jvm.org.tresamigos.smv.python.SmvPythonHelper.smvStrCat(sep, smv_copy_array(app.sc, *cols)))

def smvHashKey(head, *others):
    """Create MD5 on concatenated columns.
    Return "Prefix" + MD5 Hex string(size 32 string) as the unique key

    MD5's collisions rate on real data records could be ignored based on the following discussion.

    https://marc-stevens.nl/research/md5-1block-collision/
    The shortest messages have the same MD5 are 512-bit (64-byte) messages as below

    4dc968ff0ee35c209572d4777b721587d36fa7b21bdc56b74a3dc0783e7b9518afbfa200a8284bf36e8e4b55b35f427593d849676da0d1555d8360fb5f07fea2
    and the (different by two bits)
    4dc968ff0ee35c209572d4777b721587d36fa7b21bdc56b74a3dc0783e7b9518afbfa202a8284bf36e8e4b55b35f427593d849676da0d1d55d8360fb5f07fea2
    both have MD5 hash
    008ee33a9d58b51cfeb425b0959121c9

    There are other those pairs, but all carefully constructed.
    Theoretically the random collisions will happen on data size approaching 2^64 (since MD5 has
    128-bit), which is much larger than the number of records we deal with (a billion is about 2^30)
    There for using MD5 to hash primary key columns is good enough for creating an unique key

    This function can take 2 forms:
    - smvHashKey(prefix, col1, col2, ...)
    - smvHashKey(col1, col2, ...)

    Args:
     prefix (String): return string's prefix
     col. (Column): columns to be part of hash

    Return:
     (col): a StringType column as Prefix + MD5 Hex string
    """

    if is_string(head):
        pre = head
        cols = list(others)
    elif isinstance(head, Column):
        pre = ""
        cols = [head] + list(others)
    else:
        raise RuntimeError("first parameter must be either a String or a Column")
    app = SmvApp.getInstance()
    return Column(app._jvm.org.tresamigos.smv.python.SmvPythonHelper.smvHashKey(pre, smv_copy_array(app.sc, *cols)))
