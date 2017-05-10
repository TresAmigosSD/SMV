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
from pyspark.sql import DataFrame
from pyspark.sql.column import Column
from pyspark.sql.functions import col, lit
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
                cols (\*str): columns defining the ordering

            Returns:
                (DataFrame): result of taking top records from groups

        """
        return DataFrame(self.sgd.smvTopNRecs(maxElems, smv_copy_array(self.df._sc, *cols)), self.df.sql_ctx)

    def smvPivot(self, pivotCols, valueCols, baseOutput):
        """smvPivot adds the pivoted columns without additional aggregation. In other words
            N records in, N records out

            SmvPivot for Pivot Operations:

            Pivot Operation on DataFrame that transforms multiple rows per key into a columns so
            they can be aggregated to a single row for a given key while preserving all the data
            variance.

            Example:
                For input data below:

                +-----+-------+---------+-------+
                | id  | month | product | count |
                +=====+=======+=========+=======+
                | 1   | 5/14  |   A     |   100 |
                +-----+-------+---------+-------+
                | 1   | 6/14  |   B     |   200 |
                +-----+-------+---------+-------+
                | 1   | 5/14  |   B     |   300 |
                +-----+-------+---------+-------+

                We would like to generate a data set to be ready for aggregations.
                The desired output is:

                +-----+--------------+--------------+--------------+--------------+
                | id  | count_5_14_A | count_5_14_B | count_6_14_A | count_6_14_B |
                +=====+==============+==============+==============+==============+
                | 1   | 100          | NULL         | NULL         | NULL         |
                +-----+--------------+--------------+--------------+--------------+
                | 1   | NULL         | NULL         | NULL         | 200          |
                +-----+--------------+--------------+--------------+--------------+
                | 1   | NULL         | 300          | NULL         | NULL         |
                +-----+--------------+--------------+--------------+--------------+

                The raw input is divided into three parts.
                    1. key column: part of the primary key that is preserved in the output.
                        That would be the `id` column in the above example.
                    2. pivot columns: the columns whose row values will become the new column names.
                        The cross product of all unique values for *each* column is used to generate
                        the output column names.
                    3. value column: the value that will be copied/aggregated to corresponding output
                        column. `count` in our example.

                Example code:

                >>> df.smvGroupBy("id").smvPivot([["month", "product"]], ["count"], ["5_14_A", "5_14_B", "6_14_A", "6_14_B"])

            Args:
                pivotCols (list(list(str))): specify the pivot columns, on above example, it is
                    [['month, 'product]]. If [['month'], ['month', 'product']] is
                    used, the output columns will have "count_5_14" and "count_6_14" as
                    addition to the example.
                valueCols (list(string)): names of value columns which will be prepared for
                    aggregation
                baseOutput (list(str)): expected names of the pivoted column
                    In above example, it is ["5_14_A", "5_14_B", "6_14_A", "6_14_B"].
                    The user is required to supply the list of expected pivot column output names to avoid
                    and extra action on the input DataFrame just to extract the possible pivot columns.
                    If an empty sequence is provided, the base output columns will be extracted from
                    values in the pivot columns (will cause an action on the entire DataFrame!)

            Returns:
                (DataFrame): result of pivot operation
        """
        return DataFrame(self.sgd.smvPivot(smv_copy_array(self.df._sc, *pivotCols), smv_copy_array(self.df._sc, *valueCols), smv_copy_array(self.df._sc, *baseOutput)), self.df.sql_ctx)

    def smvPivotSum(self, pivotCols, valueCols, baseOutput):
        """Perform SmvPivot, then sum the results.
            Please refer smvPivot's document for context and details of the SmvPivot operation.

            Args:
                pivotCols (list(list(str))): list of lists of column names to pivot
                valueCols (list(string)): names of value columns to sum
                baseOutput (list(str)): expected names pivoted column

            Examples:
                For example, given a DataFrame df that represents the table

                +-----+-------+---------+-------+
                | id  | month | product | count |
                +=====+=======+=========+=======+
                | 1   | 5/14  |   A     |   100 |
                +-----+-------+---------+-------+
                | 1   | 6/14  |   B     |   200 |
                +-----+-------+---------+-------+
                | 1   | 5/14  |   B     |   300 |
                +-----+-------+---------+-------+

                we can use

                >>> df.smvGroupBy("id").smvPivotSum([["month", "product"]], ["count"], ["5_14_A", "5_14_B", "6_14_A", "6_14_B"])

                to produce the following output

                +-----+--------------+--------------+--------------+--------------+
                | id  | count_5_14_A | count_5_14_B | count_6_14_A | count_6_14_B |
                +=====+==============+==============+==============+==============+
                | 1   | 100          | 300          | NULL         | 200          |
                +-----+--------------+--------------+--------------+--------------+

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

            Returns:
                (Dataframe): result of fill nulls with previous value
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
    def method(self, *args, **kwargs):
        return getattr(helperCls(self), methodName)(*args, **kwargs)
    method.__name__ = methodName
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

    def topNValsByFreq(self, n, col):
        """Get top N most frequent values in Column c

            Args:
                n (int): maximum number of values
                col (Column): which column to get values from

            Examples:
                >>> df.topNValsByFreq(1, col("cid"))
                will return the single most frequent value in the cid column

            Returns:
                (list(object)): most frequent values (type depends on schema)
        """
        topNdf = DataFrame(self._jDfHelper._topNValsByFreq(n, col._jc), self._sql_ctx)
        return map(lambda r: r.asDict().values()[0], topNdf.collect())

    def smvSkewJoinByKey(self, other, joinType, skewVals, key):
        """Join that leverages broadcast (map-side) join of rows with skewed (high-frequency) values

            Rows keyed by skewed values are joined via broadcast join while remaining
            rows are joined without broadcast join. Occurrences of skewVals in df2 should be
            infrequent enough that the filtered table is small enough for a broadcast join.
            result is the union of the join results.

            Args:
                other (DataFrame): DataFrame to join with
                joinType (str): name of type of join (e.g. "inner")
                skewVals (list(object)): list of skewed values
                key (str): key on which to join (also the Column with the skewed values)

            Example:
                {{{
                df.smvSkewJoinByKey(df2, SmvJoinType.Inner, Seq("9999999"), "cid")
                }}}
                will broadcast join the rows of df1 and df2 where col("cid") == "9999999"
                and join the remaining rows of df1 and df2 without broadcast join.
        """
        jdf = self._jDfHelper.smvSkewJoinByKey(other._jdf, joinType, _to_seq(skewVals), key)
        return DataFrame(jdf, self._sql_ctx)

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

    def smvPrefixFieldNames(self, prefix):
        """Apply a prefix to all column names in the given `DataFrame`.

            Args:
                prefix (string): prefix string to be added to all the column names
            Example:
                >>> df.smvPrefixFieldNames("x_")

            Above will add "x_" to the beginning of every column name in the `DataFrame`.
            Please note that if the renamed column names over lap with existing columns,
            the method will error out.
        """
        jdf = self._jDfHelper.smvPrefixFieldNames(prefix)
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
        """Unions DataFrames with different number of columns by column name and schema

            Spark unionAll ignores column names & schema, and can only be performed on tables with the same number of columns.

            Args:
                dfOthers (\*DataFrame): the dataframes to union with

            Example:
                >>> df.smvUnion(df2, df3)

            Returns:
                (DataFrame): the union of all specified DataFrames
        """
        jdf = self._jDfHelper.smvUnion(_to_seq(dfothers, _jdf))
        return DataFrame(jdf, self._sql_ctx)

    def smvRenameField(self, *namePairs):
        """Rename one or more fields of a `DataFrame`

            Args:
                namePairs (\*tuple): tuples of strings where the first is the source column name, and the second is the target column name

            Example:
                >>> df.smvRenameField(("a", "aa"), ("c", "cc"))

            Returns:
                (DataFrame): the DataFrame with renamed fields
        """
        jdf = self._jPythonHelper.smvRenameField(self._jdf, smv_copy_array(self._sc, *namePairs))
        return DataFrame(jdf, self._sql_ctx)

    def smvUnpivot(self, *cols):
        """Unpivot the selected columns

            Given a set of records with value columns, turns the value columns into value rows.

            Args:
                cols (\*string): the names of the columns to unpivot

            Example:
                input DF:

                    +----+---+---+---+
                    | id | X | Y | Z |
                    +====+===+===+===+
                    | 1  | A | B | C |
                    +----+---+---+---+
                    | 2  | D | E | F |
                    +----+---+---+---+
                    | 3  | G | H | I |
                    +----+---+---+---+

                >>> df.smvUnpivot("X", "Y", "Z")

                output DF:

                    +----+--------+-------+
                    | id | column | value |
                    +====+========+=======+
                    |  1 |   X    |   A   |
                    +----+--------+-------+
                    |  1 |   Y    |   B   |
                    +----+--------+-------+
                    |  1 |   Z    |   C   |
                    +----+--------+-------+
                    | ...   ...      ...  |
                    +----+--------+-------+
                    |  3 |   Y    |   H   |
                    +----+--------+-------+
                    |  3 |   Z    |   I   |
                    +----+--------+-------+

            Returns:
                (DataFrame): the unpivoted DataFrame
        """
        jdf = self._jDfHelper.smvUnpivot(_to_seq(cols))
        return DataFrame(jdf, self._sql_ctx)

    def smvUnpivotRegex(self, cols, colNameFn, indexColName):
        """Unpivot the selected columns using the specified regex

            Args:
                cols (\*string): the names of the columns to unpivot
                colNameFn (string): a regex representing the function to be applied when unpivoting
                indexColName (string): the name of the index column to be created

            Example:
                input DF:

                    +----+-------+-------+-------+-------+
                    | id |  A_1  |  A_2  |  B_1  |  B_2  |
                    +====+=======+=======+=======+=======+
                    | 1  | 1_a_1 | 1_a_2 | 1_b_1 | 1_b_2 |
                    +----+-------+-------+-------+-------+
                    | 2  | 2_a_1 | 2_a_2 | 2_b_1 | 2_b_2 |
                    +----+-------+-------+-------+-------+

                >>> df.smvUnpivotRegex( ["A_1", "A_2", "B_1", "B_2"], "(.*)_(.*)", "index" )

                output DF:

                    +----+-------+-------+-------+
                    | id | index |   A   |   B   |
                    +====+=======+=======+=======+
                    | 1  |   1   | 1_a_1 | 1_b_1 |
                    +----+-------+-------+-------+
                    | 1  |   2   | 1_a_2 | 1_b_2 |
                    +----+-------+-------+-------+
                    | 2  |   1   | 2_a_1 | 2_b_1 |
                    +----+-------+-------+-------+
                    | 2  |   2   | 2_a_2 | 2_b_2 |
                    +----+-------+-------+-------+

            Returns:
                (DataFrame): the unpivoted DataFrame
        """
        jdf = self._jDfHelper.smvUnpivotRegex(_to_seq(cols), colNameFn, indexColName)
        return DataFrame(jdf, self._sql_ctx)

    def smvExportCsv(self, path, n=None):
        """Export DataFrame to local file system

            Args:
                path (string): relative path to the app running directory on local file system (instead of HDFS)
                n (integer): optional. number of records to export. default is all records

            Note:
                Since we have to collect the DF and then call JAVA file operations, the job have to be launched as either local or yar-client mode. Also it is user's responsibility to make sure that the DF is small enough to fit into local file system.

            Example:
                >>> df.smvExportCsv("./target/python-test-export-csv.csv")

            Returns:
                (None)
        """
        self._jDfHelper.smvExportCsv(path, n)

    def smvOverlapCheck(self, keyColName):
        """For a set of DFs, which share the same key column, check the overlap across them

            The other DataFrames are specified in another set of parentheses, as follows:

            >>> df1.smvOverlapCheck(key)(*df)

            Args:
                keyColName (string): the column name for the key column

            Examples:
                >>> df1.smvOverlapCheck("key")(df2, df3, df4)

                output DF has 2 columns:
                  - key
                  - flag: a bit-string, e.g. 0110. Each bit represents whether the original DF has this key

                It can be used with EDD to summarize on the flag:

                >>> df1.smvOverlapCheck("key")(df2, df3).smvHist("flag")
        """
        def _check(*dfothers):
            jdf = self._jPythonHelper.smvOverlapCheck(self._jdf, keyColName, smv_copy_array(self._sc, *dfothers))
            return DataFrame(jdf, self._sql_ctx)
        return _check

    def smvDesc(self, *colDescs):
        """Adds column descriptions

            Args:
                colDescs (\*tuple): tuples of strings where the first is the column name, and the second is the description to add

            Example:
                >>> df.smvDesc(("a", "description of col a"), ("b", "description of col b"))

            Returns:
                (DataFrame): the DataFrame with column descriptions added
        """
        jdf = self._jPythonHelper.smvDesc(self._jdf, smv_copy_array(self._sc, *colDescs))
        return DataFrame(jdf, self._sql_ctx)

    def smvDescFromDF(self, descDF):
        """Adds column descriptions

            Args:
                descDF (DataFrame): a companion 2-column desciptionDF that has variable names as column 1 and corresponding variable descriptions as column 2

            Example:
                >>> df.smvDescFromDF(desciptionDF)

            Returns:
                (DataFrame): the DataFrame with column descriptions added
        """
        desclist = [(str(r[0]), str(r[1])) for r in descDF.collect()]
        return self.smvDesc(*desclist)

    def smvGetDesc(self, colName = None):
        """Returns column description(s)

            Args:
                colName (string): optional column name for which to get the description.

            Example:
                >>> df.smvGetDesc("col_a")
                >>> df.smvGetDesc()

            Returns:
                (string): description string of colName, if specified
                (list(tuple)): a list of (colName, description) pairs for all columns
        """
        if (colName is not None):
            return self._jDfHelper.smvGetDesc(colName)
        else:
            return [(c, self._jDfHelper.smvGetDesc(c)) for c in self.df.columns]

    def smvRemoveDesc(self, *colNames):
        """Removes description for the given columns from the Dataframe

            Args:
                colNames (\*string): names of columns for which to remove the description

            Example:
                >>> df.smvRemoveDesc("col_a", "col_b")

            Returns:
                (DataFrame): the DataFrame with column descriptions removed
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
        """Display a DataFrame row in transposed view

            Args:
                pos (integer): the n-th row to display, default as 1
                colRegex (string): show the columns with name matching the regex, default as ".*"

            Returns:
                (None)
        """
        self._println(self._peekStr(pos, colRegex))

    def peekSave(self, path, pos = 1,  colRegex = ".*"):
        """Write `peek` result to a file

            Args:
                path (string): local file name to Write
                pos (integer): the n-th row to display, default as 1
                colRegex (string): show the columns with name matching the regex, default as ".*"

            Returns:
                (None)
        """
        self._printFile(path, self._peekStr(pos, colRegex))

    def _smvEdd(self, *cols):
        return self._jDfHelper._smvEdd(_to_seq(cols)).createReport()

    def smvEdd(self, *cols):
        """Display EDD summary

            Args:
                cols (\*string): column names on which to perform EDD summary

            Example:
                >>> df.smvEdd("a", "b")

            Returns:
                (None)
        """
        self._println(self._smvEdd(*cols))

    def _smvHist(self, *cols):
        return self._jDfHelper._smvHist(_to_seq(cols)).createReport()

    def smvHist(self, *cols):
        """Display EDD histogram

            Each column's histogram prints separately

            Args:
                cols (\*string): The columns on which to perform EDD histogram

            Example:
                >>> df.smvHist("a")

            Returns:
                (None)
        """
        self._println(self._smvHist(*cols))

    def _smvConcatHist(self, *cols):
        return self._jPythonHelper.smvConcatHist(self._jdf, smv_copy_array(self._sc, *cols)).createReport()

    def smvConcatHist(self, *cols):
        """Display EDD histogram of a group of columns (joint distribution)

            Args:
                cols (\*string): The columns on which to perform EDD histogram

            Example:
                >>> df.smvConcatHist("a", "b")

            Returns:
                (None)
        """
        self._println(self._smvConcatHist(*cols))

    def _smvFreqHist(self, *cols):
        return self._jDfHelper._smvFreqHist(_to_seq(cols)).createReport()

    def smvFreqHist(self, *cols):
        """Print EDD histogram with frequency sorting

            Args:
                cols (\*string): The columns on which to perform EDD histogram

            Example:
                >>> df.smvFreqHist("a")

            Returns:
                (None)
        """
        self._println(self._smvFreqHist(*cols))

    def _smvCountHist(self, keys, binSize):
        if isinstance(keys, basestring):
            res = self._jDfHelper._smvCountHist(_to_seq([keys]), binSize)
        else:
            res = self._jDfHelper._smvCountHist(_to_seq(keys), binSize)
        return res.createReport()

    def smvCountHist(self, keys, binSize):
        """Print the distribution of the value frequency on specific columns

            Args:
                keys (list(string)): the column names on which the EDD histogram is performed
                binSize (integer): the bin size for the histogram

            Example:
                >>> df.smvCountHist(["k"], 1)

            Returns:
                (None)
        """
        self._println(self._smvCountHist(keys, binSize))

    def _smvBinHist(self, *colWithBin):
        for elem in colWithBin:
            assert type(elem) is tuple, "smvBinHist takes a list of tuple(string, double) as paraeter"
            assert len(elem) == 2, "smvBinHist takes a list of tuple(string, double) as parameter"
        insureDouble = map(lambda t: (t[0], t[1] * 1.0), colWithBin)
        return self._jPythonHelper.smvBinHist(self._jdf, smv_copy_array(self._sc, *insureDouble)).createReport()

    def smvBinHist(self, *colWithBin):
        """Print distributions on numerical columns with applying the specified bin size

            Args:
                colWithBin (\*tuple): each tuple must be of size 2, where the first element is the column name, and the second is the bin size for that column

            Example:
                >>> df.smvBinHist(("col_a", 1))

            Returns:
                (None)
        """
        self._println(self._smvBinHist(*colWithBin))

    def _smvEddCompare(self, df2, ignoreColName):
        return self._jDfHelper._smvEddCompare(df2._jdf, ignoreColName)

    def smvEddCompare(self, df2, ignoreColName):
        """Compare 2 DFs by comparing their Edd Summary result

            The result of the comparison is printed out.

            Args:
                df2 (DataFrame): the DataFrame to compare with
                ignoreColName (boolean): specifies whether to ignore column names, default is false

            Example:
                >>> df.smvEddCompare(df2)
                >>> df.smvEddCompare(df2, true)

            Returns:
                (None)
        """
        self._println(self._smvEddCompare(df2, ignoreColName))

    def _smvDiscoverPK(self, n):
        pk = self._jPythonHelper.smvDiscoverPK(self._jdf, n)
        return "[{}], {}".format(", ".join(map(str, pk._1())), pk._2())

    def smvDiscoverPK(self, n=10000):
        """Find a column combination which uniquely identifies a row from the data

            Note:
                The algorithm only looks for a set of keys which uniquely identifies the row. There could be more key combinations which can also be the primary key.

            Args:
                n (integer): number of rows the PK discovery algorithm will run on, defaults to 10000

            Example:
                >>> df.smvDiscoverPK(5000)

            Returns:
                (None)

        """
        self._println(self._smvDiscoverPK(n))

    def smvDumpDF(self):
        """Dump the schema and data of given df to screen for debugging purposes

            Similar to `show()` method of DF from Spark 1.3, although the format is slightly different.  This function's format is more convenient for us and hence has remained.

            Example:
                >>> df.smvDumpDF()

            Returns:
                (None)
        """
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
        """Returns true if ALL of the Array columns' elements are in the given parameter sequence

            Args:
                vals (\*any): vals must be of the same type as the Array content

            Example:
                input DF:

                    +---+---+
                    | k | v |
                    +===+===+
                    | a | b |
                    +---+---+
                    | c | d |
                    +---+---+
                    |   |   |
                    +---+---+

                >>> df.select(array(col("k"), col("v")).smvIsAllIn("a", "b", "c").alias("isFound"))

                output DF:

                    +---------+
                    | isFound |
                    +=========+
                    |  true   |
                    +---------+
                    |  false  |
                    +---------+
                    |  false  |
                    +---------+

            Returns:
                (Column): BooleanType
        """
        jc = self._jPythonHelper.smvIsAllIn(self._jc, _to_seq(vals))
        return Column(jc)

    def smvIsAnyIn(self, *vals):
        """Returns true if ANY one of the Array columns' elements are in the given parameter sequence

            Args:
                vals (\*any): vals must be of the same type as the Array content

            Example:
                input DF:

                    +---+---+
                    | k | v |
                    +===+===+
                    | a | b |
                    +---+---+
                    | c | d |
                    +---+---+
                    |   |   |
                    +---+---+

                >>> df.select(array(col("k"), col("v")).smvIsAnyIn("a", "b", "c").alias("isFound"))

                output DF:

                    +---------+
                    | isFound |
                    +=========+
                    |  true   |
                    +---------+
                    |  true   |
                    +---------+
                    |  false  |
                    +---------+

            Returns:
                (Column): BooleanType
        """
        jc = self._jPythonHelper.smvIsAnyIn(self._jc, _to_seq(vals))
        return Column(jc)

    def smvMonth(self):
        """Extract month component from a timestamp

            Example:
                >>> df.select(col("dob").smvMonth())

            Returns:
                (Column): IntegerType. Month component as integer, or null if input column is null
        """
        jc = self._jColumnHelper.smvMonth()
        return Column(jc)

    def smvYear(self):
        """Extract year component from a timestamp

            Example:
                >>> df.select(col("dob").smvYear())

            Returns:
                (Column): IntegerType. Year component as integer, or null if input column is null
        """
        jc = self._jColumnHelper.smvYear()
        return Column(jc)

    def smvQuarter(self):
        """Extract quarter component from a timestamp

            Example:
                >>> df.select(col("dob").smvQuarter())

            Returns:
                (Column): IntegerType. Quarter component as integer (1-based), or null if input column is null
        """
        jc = self._jColumnHelper.smvQuarter()
        return Column(jc)

    def smvDayOfMonth(self):
        """Extract day of month component from a timestamp

            Example:
                >>> df.select(col("dob").smvDayOfMonth())

            Returns:
                (Column): IntegerType. Day of month component as integer (range 1-31), or null if input column is null
        """
        jc = self._jColumnHelper.smvDayOfMonth()
        return Column(jc)

    def smvDayOfWeek(self):
        """Extract day of week component from a timestamp

            Example:
                >>> df.select(col("dob").smvDayOfWeek())

            Returns:
                (Column): IntegerType. Day of week component as integer (range 1-7, 1 being Sunday), or null if input column is null
        """
        jc = self._jColumnHelper.smvDayOfWeek()
        return Column(jc)

    def smvHour(self):
        """Extract hour component from a timestamp

            Example:
                >>> df.select(col("dob").smvHour())

            Returns:
                (Column): IntegerType. Hour component as integer, or null if input column is null
        """
        jc = self._jColumnHelper.smvHour()
        return Column(jc)

    def smvPlusDays(self, delta):
        """Add N days to `Timestamp` column

            Args:
                delta (integer): the number of days to add

            Example:
                >>> df.select(col("dob").smvPlusDays(3))

            Returns:
                (Column): TimestampType. The incremented Timestamp, or null if input is null
        """
        if (isinstance(delta, int)):
            jdelta = delta
        elif (isinstance(delta, Column)):
            jdelta = delta._jc
        else:
            raise RuntimeError("delta parameter must be either an int or a Column")
        jc = self._jColumnHelper.smvPlusDays(jdelta)
        return Column(jc)

    def smvPlusWeeks(self, delta):
        """Add N weeks to `Timestamp` column

            Args:
                delta (integer): the number of weeks to add

            Example:
                >>> df.select(col("dob").smvPlusWeeks(3))

            Returns:
                (Column): TimestampType. The incremented Timestamp, or null if input is null
        """
        if (isinstance(delta, int)):
            jdelta = delta
        elif (isinstance(delta, Column)):
            jdelta = delta._jc
        else:
            raise RuntimeError("delta parameter must be either an int or a Column")
        jc = self._jColumnHelper.smvPlusWeeks(jdelta)
        return Column(jc)

    def smvPlusMonths(self, delta):
        """Add N months to `Timestamp` column

            Args:
                delta (integer): the number of months to add

            Note:
                The calculation will do its best to only change the month field retaining the same day of month. However, in certain circumstances, it may be necessary to alter smaller fields. For example, 2007-03-31 plus one month cannot result in 2007-04-31, so the day of month is adjusted to 2007-04-30.

            Example:
                >>> df.select(col("dob").smvPlusMonths(3))

            Returns:
                (Column): TimestampType. The incremented Timestamp, or null if input is null
        """
        if (isinstance(delta, int)):
            jdelta = delta
        elif (isinstance(delta, Column)):
            jdelta = delta._jc
        else:
            raise RuntimeError("delta parameter must be either an int or a Column")
        jc = self._jColumnHelper.smvPlusMonths(jdelta)
        return Column(jc)

    def smvPlusYears(self, delta):
        """Add N years to `Timestamp` column

            Args:
                delta (integer): the number of years to add

            Example:
                >>> df.select(col("dob").smvPlusYears(3))

            Returns:
                (Column): TimestampType. The incremented Timestamp, or null if input is null
        """
        if (isinstance(delta, int)):
            jdelta = delta
        elif (isinstance(delta, Column)):
            jdelta = delta._jc
        else:
            raise RuntimeError("delta parameter must be either an int or a Column")
        jc = self._jColumnHelper.smvPlusYears(jdelta)
        return Column(jc)

    def smvStrToTimestamp(self, fmt):
        """Build a timestamp from a string

            Args:
                fmt (string): the format is the same as the Java `Date` format

            Example:
                >>> df.select(col("dob").smvStrToTimestamp("yyyy-MM-dd"))

            Returns:
                (Column): TimestampType. The converted Timestamp
        """
        jc = self._jColumnHelper.smvStrToTimestamp(fmt)
        return Column(jc)

    def smvDay70(self):
        """Convert a Timestamp to the number of days from 1970-01-01

            Example:
                >>> df.select(col("dob").smvDay70())

            Returns:
                (Column): IntegerType. Number of days from 1970-01-01 (start from 0)
        """
        jc = self._jColumnHelper.smvDay70()
        return Column(jc)

    def smvMonth70(self):
        """Convert a Timestamp to the number of months from 1970-01-01

            Example:
                >>> df.select(col("dob").smvMonth70())

            Returns:
                (Column): IntegerType. Number of months from 1970-01-01 (start from 0)
        """
        jc = self._jColumnHelper.smvMonth70()
        return Column(jc)


_helpCls(Column, ColumnHelper)
