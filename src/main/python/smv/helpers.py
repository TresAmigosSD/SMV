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
"""SMV DataFrame Helpers and Column Helpers

This module provides the helper functions on DataFrame objects and Column objects
"""

from pyspark import SparkContext
from pyspark.sql import HiveContext, DataFrame
from pyspark.sql.column import Column
from pyspark.sql.functions import col
from utils import smv_copy_array

import sys
import inspect

# common converters to pass to _to_seq and _to_list
def _jcol(c): return c._jc
def _jdf(df): return df._jdf

# Modified from Spark column.py
def _to_seq(cols, converter=None):
    """
    Convert a list of Column (or names) into a JVM Seq of Column.

    An optional `converter` could be used to convert items in `cols`
    into JVM Column objects.
    """
    if converter:
        cols = [converter(c) for c in cols]
    return _sparkContext()._jvm.PythonUtils.toSeq(cols)

# Modified from Spark column.py
def _to_list(cols, converter=None):
    """
    Convert a list of Column (or names) into a JVM (Scala) List of Column.

    An optional `converter` could be used to convert items in `cols`
    into JVM Column objects.
    """
    if converter:
        cols = [converter(c) for c in cols]
    return _sparkContext()._jvm.PythonUtils.toList(cols)

def _sparkContext():
    return SparkContext._active_spark_context

class SmvGroupedData(object):
    """The result of running `smvGroupBy` on a DataFrame. Implements special SMV aggregations.
    """
    def __init__(self, df, sgd):
        self.df = df
        self.sgd = sgd

    def smvTopNRecs(self, maxElems, *cols):
        """For each group, return the top N records according to a given ordering

            Example:
                # This will keep the 3 largest amt records for each id
                df.smvGroupBy("id").smvTopNRecs(3, col("amt").desc())

            Args:
                maxElems (int): maximum number of records per group
                cols (*str): columns defining the ordering

            Returns:
                (DataFrame): result of taking top records from groups

        """
        return DataFrame(self.sgd.smvTopNRecs(maxElems, smv_copy_array(self.df._sc, *cols)), self.df.sql_ctx)

    def smvPivotSum(self, pivotCols, valueCols, baseOutput):
        """Perform SmvPivot, then sum the results

            The user is required to supply the list of expected pivot column
            output names to avoid extra action on the input DataFrame. If an
            empty sequence is provided, then the base output columns will be
            extracted from values in the pivot columns (will cause an action
            on the entire DataFrame!)

            Args:
                pivotCols (list(list(str))): lists of names of column names to pivot
                valueCols (list(string)): names of value columns to sum
                baseOutput (list(str)): expected names pivoted column

            Examples:
                For example, given a DataFrame df that represents the table

                | id  | month | product | count |
                | --- | ----- | ------- | ----- |
                | 1   | 5/14  |   A     |   100 |
                | 1   | 6/14  |   B     |   200 |
                | 1   | 5/14  |   B     |   300 |

                we can use

                >>> df.smvGroupBy("id").smvPivotSum(Seq("month", "product"))("count")("5_14_A", "5_14_B", "6_14_A", "6_14_B")

                to produce the following output

                | id  | count_5_14_A | count_5_14_B | count_6_14_A | count_6_14_B |
                | --- | ------------ | ------------ | ------------ | ------------ |
                | 1   | 100          | 300          | NULL         | 200          |

            Returns:
                (DataFrame): result of pivot sum
        """
        return DataFrame(self.sgd.smvPivotSum(smv_copy_array(self.df._sc, *pivotCols), smv_copy_array(self.df._sc, *valueCols), smv_copy_array(self.df._sc, *baseOutput)), self.df.sql_ctx)

    def smvPivotCoalesce(self, pivotCols, valueCols, baseOutput):
        """Perform SmvPivot, then coalesce the output

            Args:
                pivotCols (list(list(str))): lists of names of column names to pivot
                valueCols (list(string)): names of value columns to coalesce
                baseOutput (list(str)): expected names pivoted column

            Returns:
                (Dataframe): result of pivot coalesce
        """
        return DataFrame(self.sgd.smvPivotCoalesce(smv_copy_array(self.df._sc, *pivotCols), smv_copy_array(self.df._sc, *valueCols), smv_copy_array(self.df._sc, *baseOutput)), self.df.sql_ctx)

    def smvFillNullWithPrevValue(self, *orderCols):
        """Fill in Null values with "previous" value according to an ordering

            Examples:
                Given DataFrame df representing the table

                K, T, V
                a, 1, null
                a, 2, a
                a, 3, b
                a, 4, null

                we can use

                >>> df.smvGroupBy("K").smvFillNullWithPrevValue($"T".asc)("V")

                to preduce the result

                K, T, V
                a, 1, null
                a, 2, a
                a, 3, b
                a, 4, b
        """
        def __doFill(*valueCols):
            return DataFrame(self.sgd.smvFillNullWithPrevValue(smv_copy_array(self.df._sc, *orderCols), smv_copy_array(self.df._sc, *valueCols)), self.df.sql_ctx)
        return __doFill

class SmvMultiJoin(object):
    """Wrapper around Scala's SmvMultiJoin"""
    def __init__(self, sqlContext, mj):
        self.sqlContext = sqlContext
        self.mj = mj

    def joinWith(self, df, postfix, jointype = None):
        """Append SmvMultiJoin Chain

            Args:
                df (DataFrame): the DataFrame to join with
                postfix (string): postfix to use when renaming join columns
                jointype (string): optional jointype. if not specified, `conf.defaultJoinType` is used.  choose one of ['inner', 'outer', 'leftouter', 'rightouter', 'leftsemi']

            Example:
                >>> joindf = df1.smvJoinMultipleByKey(['a'], 'inner').joinWith(df2, '_df2').joinWith(df3, '_df3', 'outer')

            Returns:
                (SmvMultiJoin): formula of the join. need to call `doJoin()` on it to execute
        """
        return SmvMultiJoin(self.sqlContext, self.mj.joinWith(df._jdf, postfix, jointype))

    def doJoin(self, dropextra = False):
        """Trigger the join operation

            Args:
                dropExtra (boolean): default false, which will keep all duplicated name columns with the postfix.
                                     when true, the duplicated columns will be dropped

            Example:
                joindf.doJoin()

            Returns:
                (DataFrame): result of executing the join operation
        """
        return DataFrame(self.mj.doJoin(dropextra), self.sqlContext)

def _getUnboundMethod(helperCls, methodName):
    def method(self, *args):
        return getattr(helperCls(self), methodName)(*args)
    return method

def _helpCls(receiverCls, helperCls):
    for name, method in inspect.getmembers(helperCls, predicate=inspect.ismethod):
        # ignore special and private methods
        if not name.startswith("_"):
            newMethod = _getUnboundMethod(helperCls, name)
            setattr(receiverCls, name, newMethod)

class DataFrameHelper(object):
    def __init__(self, df):
        self.df = df
        self._sc = df._sc
        self._sql_ctx = df.sql_ctx
        self._jdf = df._jdf
        self._jPythonHelper = df._sc._jvm.SmvPythonHelper
        self._jDfHelper = df._sc._jvm.SmvDFHelper(df._jdf)

    def smvExpandStruct(self, *cols):
        """Expand structure type column to a group of columns

            Args:
                cols (\*string): column names to expand

            Example:
                input DF:
                    [id: string, address: struct<state:string, zip:string, street:string>]

                >>> df.smvExpandStruct("address")

                output DF:
                    [id: string, state: string, zip: string, street: string]

            Returns:
                (DataFrame): DF with expanded columns
        """
        jdf = self._jPythonHelper.smvExpandStruct(self._jdf, smv_copy_array(self._sc, *cols))
        return DataFrame(jdf, self._sql_ctx)

    def smvGroupBy(self, *cols):
        """Similar to groupBy, instead of creating GroupedData, create an `SmvGroupedData` object.

            See [[org.tresamigos.smv.SmvGroupedDataFunc]] for list of functions that can be applied to the grouped data.

            Args:
                cols (\*string or \*Column): column names or Column objects to group on

            Note:
                This is going away shortly and user will be able to use standard Spark `groupBy` method directly.

            Example:
                >>> df.smvGroupBy(col("k"))
                >>> df.smvGroupBy("k")

            Returns:
                (SmvGroupedData): grouped data object

        """
        jSgd = self._jPythonHelper.smvGroupBy(self._jdf, smv_copy_array(self._sc, *cols))
        return SmvGroupedData(self.df, jSgd)

    def smvHashSample(self, key, rate=0.01, seed=23):
        """Sample the df according to the hash of a column

            MurmurHash3 algorithm is used for generating the hash

            Args:
                key (string or Column): column name or Column to sample on
                rate (double): sample rate in range (0, 1] with a default of 0.01 (1%)
                seed (int): random generator integer seed with a default of 23

            Example:
                >>> df.smvHashSample(col("key"), rate=0.1, seed=123)

            Returns:
                (DataFrame): sampled DF
        """
        if (isinstance(key, basestring)):
            jkey = col(key)._jc
        elif (isinstance(key, Column)):
            jkey = key._jc
        else:
            raise RuntimeError("key parameter must be either a String or a Column")

        jdf = self._jDfHelper.smvHashSample(jkey, rate, seed)

        return DataFrame(jdf, self._sql_ctx)

    # FIXME py4j method resolution with null argument can fail, so we
    # temporarily remove the trailing parameters till we can find a
    # workaround
    def smvJoinByKey(self, other, keys, joinType):
        """joins two DataFrames on a key

            The Spark `DataFrame` join operation does not handle duplicate key names.
            If both left and right side of the join operation contain the same key,
            the result `DataFrame` is unusable.

            The `smvJoinByKey` method will allow the user to join two `DataFrames` using the same join key.
            Post join, only the left side keys will remain. In case of outer-join, the
            `coalesce(leftkey, rightkey)` will replace the left key to be kept.

            Args:
                other (DataFrame): the DataFrame to join with
                keys (list(string)): a list of column names on which to apply the join
                joinType (string): choose one of ['inner', 'outer', 'leftouter', 'rightouter', 'leftsemi']

            Example:
                >>> df1.smvJoinByKey(df2, ["k"], "inner")

            Returns:
                (DataFrame): result of the join operation
        """
        jdf = self._jPythonHelper.smvJoinByKey(self._jdf, other._jdf, _to_seq(keys), joinType)
        return DataFrame(jdf, self._sql_ctx)

    def smvJoinMultipleByKey(self, keys, joinType = 'inner'):
        """Create multiple DF join builder

            It is used in conjunction with `joinWith` and `doJoin`

            Args:
                keys (list(string)): a list of column names on which to apply the join
                joinType (string): choose one of ['inner', 'outer', 'leftouter', 'rightouter', 'leftsemi']

            Example:
                >>> df.joinMultipleByKey(["k1", "k2"], "inner").joinWith(df2, "_df2").joinWith(df3, "_df3", "leftouter").doJoin()

            Returns:
                (SmvMultiJoin): the builder object for the multi join operation
        """
        jdf = self._jPythonHelper.smvJoinMultipleByKey(self._jdf, smv_copy_array(self._sc, *keys), joinType)
        return SmvMultiJoin(self._sql_ctx, jdf)

    def smvSelectMinus(self, *cols):
        """Remove one or more columns from current DataFrame

            Args:
                cols (\*string or \*Column): column names or Columns to remove from the DataFrame

            Example:
                >>> df.smvSelectMinus("col1", "col2")
                >>> df.smvSelectMinus(col("col1"), col("col2"))

            Returns:
                (DataFrame): the resulting DataFrame after removal of columns
        """
        jdf = self._jPythonHelper.smvSelectMinus(self._jdf, smv_copy_array(self._sc, *cols))
        return DataFrame(jdf, self._sql_ctx)

    def smvSelectPlus(self, *cols):
        """Selects all the current columns in current DataFrame plus the supplied expressions

            The new columns are added to the end of the current column list.

            Args:
                cols (\*Column): expressions to add to the DataFrame

            Example:
                >>> df.smvSelectPlus(col("price") * col("count") as "amt")

            Returns:
                (DataFrame): the resulting DataFrame after removal of columns
        """
        jdf = self._jDfHelper.smvSelectPlus(_to_seq(cols, _jcol))
        return DataFrame(jdf, self._sql_ctx)

    def smvDedupByKey(self, *keys):
        """Remove duplicate records from the DataFrame by arbitrarly selecting the first record from a set of records with same primary key or key combo.

            Args:
                keys (\*string or \*Column): the column names or Columns on which to apply dedup

            Example:
                input DataFrame:

                +-----+---------+---------+
                | id  | product | Company |
                +=====+=========+=========+
                | 1   | A       | C1      |
                +-----+---------+---------+
                | 1   | C       | C2      |
                +-----+---------+---------+
                | 2   | B       | C3      |
                +-----+---------+---------+
                | 2   | B       | C4      |
                +-----+---------+---------+

                >>> df.dedupByKey("id")

                output DataFrame:

                +-----+---------+---------+
                | id  | product | Company |
                +=====+=========+=========+
                | 1   | A       | C1      |
                +-----+---------+---------+
                | 2   | B       | C3      |
                +-----+---------+---------+

                >>> df.dedupByKey("id", "product")

                output DataFrame:

                +-----+---------+---------+
                | id  | product | Company |
                +=====+=========+=========+
                | 1   | A       | C1      |
                +-----+---------+---------+
                | 1   | C       | C2      |
                +-----+---------+---------+
                | 2   | B       | C3      |
                +-----+---------+---------+

            Returns:
                (DataFrame): a DataFrame without duplicates for the specified keys
        """
        jdf = self._jPythonHelper.smvDedupByKey(self._jdf, smv_copy_array(self._sc, *keys))
        return DataFrame(jdf, self._sql_ctx)

    def smvDedupByKeyWithOrder(self, *keys):
        """Remove duplicated records by selecting the first record relative to a given ordering

            The order is specified in another set of parentheses, as follows:

            >>> def smvDedupByKeyWithOrder(self, *keys)(*orderCols)

            Note:
                Same as the `dedupByKey` method, we use RDD groupBy in the implementation of this method to make sure we can handle large key space.

            Args:
                keys (\*string or \*Column): the column names or Columns on which to apply dedup

            Example:
                input DataFrame:

                +-----+---------+---------+
                | id  | product | Company |
                +=====+=========+=========+
                | 1   | A       | C1      |
                +-----+---------+---------+
                | 1   | C       | C2      |
                +-----+---------+---------+
                | 2   | B       | C3      |
                +-----+---------+---------+
                | 2   | B       | C4      |
                +-----+---------+---------+

                >>> df.dedupByKeyWithOrder(col("id"))(col("product").desc())

                output DataFrame:

                +-----+---------+---------+
                | id  | product | Company |
                +=====+=========+=========+
                | 1   | C       | C2      |
                +-----+---------+---------+
                | 2   | B       | C3      |
                +-----+---------+---------+

            Returns:
                (DataFrame): a DataFrame without duplicates for the specified keys / order
        """
        def _withOrder(*orderCols):
            jdf = self._jPythonHelper.smvDedupByKeyWithOrder(self._jdf, smv_copy_array(self._sc, *keys), smv_copy_array(self._sc, *orderCols))
            return DataFrame(jdf, self._sql_ctx)
        return _withOrder

    def smvUnion(self, *dfothers):
        jdf = self._jDfHelper.smvUnion(_to_seq(dfothers, _jdf))
        return DataFrame(jdf, self._sql_ctx)

    def smvRenameField(self, *namePairs):
        jdf = self._jPythonHelper.smvRenameField(self._jdf, smv_copy_array(self._sc, *namePairs))
        return DataFrame(jdf, self._sql_ctx)

    def smvUnpivot(self, *cols):
        jdf = self._jDfHelper.smvUnpivot(_to_seq(cols))
        return DataFrame(jdf, self._sql_ctx)

    def smvUnpivotRegex(self, cols, colNameFn, indexColName):
        jdf = self._jDfHelper.smvUnpivotRegex(_to_seq(cols), colNameFn, indexColName)
        return DataFrame(jdf, self._sql_ctx)

    def smvExportCsv(self, path, n=None):
        self._jDfHelper.smvExportCsv(path, n)

    def smvOverlapCheck(self, keyColName):
        def _check(*dfothers):
            jdf = self._jPythonHelper.smvOverlapCheck(self._jdf, keyColName, smv_copy_array(self._sc, *dfothers))
            return DataFrame(jdf, self._sql_ctx)
        return _check

    def smvDesc(self, *colDescs):
        """Adds column descriptions

        Example:
        val res = df.smvDesc(
          ("name", "This is customer's name"),
          ("sex", "This is customer\'s self-identified sex")
        )
        """
        jdf = self._jPythonHelper.smvDesc(self._jdf, smv_copy_array(self._sc, *colDescs))
        return DataFrame(jdf, self._sql_ctx)

    def smvDescFromDF(self, descDF):
        desclist = [(str(r[0]), str(r[1])) for r in descDF.collect()]
        return self.smvDesc(*desclist)

    def smvGetDesc(self, colName = None):
        """Return column description(s)

        If colName specified, will return the Description string, if not specified,
        will return a list of (colName, description) pairs
        """
        if (colName is not None):
            return self._jDfHelper.smvGetDesc(colName)
        else:
            return [(c, self._jDfHelper.smvGetDesc(c)) for c in self.df.columns]

    def smvRemoveDesc(self, *colNames):
        """Return a Dataframe with the Description removed from the given columns
        """
        jdf = self._jPythonHelper.smvRemoveDesc(self._jdf, smv_copy_array(self._sc, *colNames))
        return DataFrame(jdf, self._sql_ctx)

    #############################################
    # DfHelpers which print to STDOUT
    # Scala side which print to STDOUT will not work on Jupyter. Have to pass the string to python side then print to stdout
    #############################################
    def _println(self, string):
        sys.stdout.write(string + "\n")

    def _printFile(self, f, str):
        tgt = open(f, "w")
        tgt.write(str + "\n")
        tgt.close()

    def _peekStr(self, pos = 1, colRegex = ".*"):
        return self._jPythonHelper.peekStr(self._jdf, pos, colRegex)

    def peek(self, pos = 1, colRegex = ".*"):
        self._println(self._peekStr(pos, colRegex))

    def peekSave(self, path, pos = 1,  colRegex = ".*"):
        self._printFile(path, self._peekStr(pos, colRegex))

    def _smvEdd(self, *cols):
        return self._jDfHelper._smvEdd(_to_seq(cols))

    def smvEdd(self, *cols):
        self._println(self._smvEdd(*cols))

    def _smvHist(self, *cols):
        return self._jDfHelper._smvHist(_to_seq(cols))

    def smvHist(self, *cols):
        self._println(self._smvHist(*cols))

    def _smvConcatHist(self, *cols):
        return self._jPythonHelper.smvConcatHist(self._jdf, smv_copy_array(self._sc, *cols))

    def smvConcatHist(self, *cols):
        self._println(self._smvConcatHist(*cols))

    def _smvFreqHist(self, *cols):
        return self._jDfHelper._smvFreqHist(_to_seq(cols))

    def smvFreqHist(self, *cols):
        self._println(self._smvFreqHist(*cols))

    def _smvCountHist(self, keys, binSize):
        if isinstance(keys, basestring):
            res = self._jDfHelper._smvCountHist(_to_seq([keys]), binSize)
        else:
            res = self._jDfHelper._smvCountHist(_to_seq(keys), binSize)
        return res

    def smvCountHist(self, keys, binSize):
        self._println(self._smvCountHist(keys, binSize))

    def _smvBinHist(self, *colWithBin):
        for elem in colWithBin:
            assert type(elem) is tuple, "smvBinHist takes a list of tuple(string, double) as paraeter"
            assert len(elem) == 2, "smvBinHist takes a list of tuple(string, double) as parameter"
        insureDouble = map(lambda t: (t[0], t[1] * 1.0), colWithBin)
        return self._jPythonHelper.smvBinHist(self._jdf, smv_copy_array(self._sc, *insureDouble))

    def smvBinHist(self, *colWithBin):
        self._println(self._smvBinHist(*colWithBin))

    def _smvEddCompare(self, df2, ignoreColName):
        return self._jDfHelper._smvEddCompare(df2._jdf, ignoreColName)

    def smvEddCompare(self, df2, ignoreColName):
        self._println(self._smvEddCompare(df2, ignoreColName))

    def _smvDiscoverPK(self, n):
        pk = self._jPythonHelper.smvDiscoverPK(self._jdf, n)
        return "[{}], {}".format(", ".join(map(str, pk._1())), pk._2())

    def smvDiscoverPK(self, n=10000):
        self._println(self._smvDiscoverPK(n))

    def smvDumpDF(self):
        self._println(self._jDfHelper._smvDumpDF())

_helpCls(DataFrame, DataFrameHelper)

class ColumnHelper(object):
    def __init__(self, col):
        self.col = col
        self._jc = col._jc
        self._jvm = _sparkContext()._jvm
        self._jPythonHelper = self._jvm.SmvPythonHelper
        self._jColumnHelper = self._jvm.ColumnHelper(self._jc)

    def smvIsAllIn(self, *vals):
        jc = self._jPythonHelper.smvIsAllIn(self._jc, _to_seq(vals))
        return Column(jc)

    def smvIsAnyIn(self, *vals):
        jc = self._jPythonHelper.smvIsAnyIn(self._jc, _to_seq(vals))
        return Column(jc)

    def smvMonth(self):
        jc = self._jColumnHelper.smvMonth()
        return Column(jc)

    def smvYear(self):
        jc = self._jColumnHelper.smvYear()
        return Column(jc)

    def smvQuarter(self):
        jc = self._jColumnHelper.smvQuarter()
        return Column(jc)

    def smvDayOfMonth(self):
        jc = self._jColumnHelper.smvDayOfMonth()
        return Column(jc)

    def smvDayOfWeek(self):
        jc = self._jColumnHelper.smvDayOfWeek()
        return Column(jc)

    def smvHour(self):
        jc = self._jColumnHelper.smvHour()
        return Column(jc)

    def smvPlusDays(self, delta):
        jc = self._jColumnHelper.smvPlusDays(delta)
        return Column(jc)

    def smvPlusWeeks(self, delta):
        jc = self._jColumnHelper.smvPlusWeeks(delta)
        return Column(jc)

    def smvPlusMonths(self, delta):
        jc = self._jColumnHelper.smvPlusMonths(delta)
        return Column(jc)

    def smvPlusYears(self, delta):
        jc = self._jColumnHelper.smvPlusYears(delta)
        return Column(jc)

    def smvStrToTimestamp(self, fmt):
        jc = self._jColumnHelper.smvStrToTimestamp(fmt)
        return Column(jc)

    def smvDay70(self):
        jc = self._jColumnHelper.smvDay70()
        return Column(jc)

    def smvMonth70(self):
        jc = self._jColumnHelper.smvMonth70()
        return Column(jc)


_helpCls(Column, ColumnHelper)
