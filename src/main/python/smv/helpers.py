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
import sys
import inspect

import decorator
from pyspark import SparkContext
from pyspark.sql import DataFrame
from pyspark.sql.column import Column
import pyspark.sql.functions as F
from pyspark.sql.types import DataType

from smv.utils import smv_copy_array
from smv.error import SmvRuntimeError
from smv.utils import is_string
from smv.schema_meta_ops import SchemaMetaOps


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
    def __init__(self, df, keys, sgd):
        self.df = df
        self.keys = keys
        self.sgd = sgd

    def smvTopNRecs(self, maxElems, *cols):
        """For each group, return the top N records according to a given ordering

            Example:

                >>> df.smvGroupBy("id").smvTopNRecs(3, col("amt").desc())

                This will keep the 3 largest amt records for each id

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
            Please refer smvPivot's document for context and details of the SmvPivot operation.

            Args:
                pivotCols (list(list(str))): list of lists of column names to pivot
                valueCols (list(string)): names of value columns to coalesce
                baseOutput (list(str)): expected names pivoted column

            Examples:
                For example, given a DataFrame df that represents the table

                +---+---+----+
                |  k|  p|   v|
                +===+===+====+
                |  a|  c|   1|
                +---+---+----+
                |  a|  d|   2|
                +---+---+----+
                |  a|  e|null|
                +---+---+----+
                |  a|  f|   5|
                +---+---+----+

                we can use

                >>> df.smvGroupBy("k").smvPivotCoalesce([['p']], ['v'], ['c', 'd', 'e', 'f'])

                to produce the following output

                +---+---+---+----+---+
                |  k|v_c|v_d| v_e|v_f|
                +===+===+===+====+===+
                |  a|  1|  2|null|  5|
                +---+---+---+----+---+

            Returns:
                (Dataframe): result of pivot coalesce
        """
        return DataFrame(self.sgd.smvPivotCoalesce(smv_copy_array(self.df._sc, *pivotCols), smv_copy_array(self.df._sc, *valueCols), smv_copy_array(self.df._sc, *baseOutput)), self.df.sql_ctx)

    def smvRePartition(self, numParts):
        """Repartition SmvGroupedData using specified partitioner on the keys. A
            HashPartitioner with the specified number of partitions will be used.

            This method is used in the cases that the key-space is very large. In the
            current Spark DF's groupBy method, the entire key-space is actually loaded
            into executor's memory, which is very dangerous when the key space is big.
            The regular DF's repartition function doesn't solve this issue since a random
            repartition will not guaranteed to reduce the key-space on each executor.
            In that case we need to use this function to linearly reduce the key-space.

            Example:

            >>> df.smvGroupBy("k1", "k2").smvRePartition(32).agg(sum("v") as "v")
        """
        jgdadp = self.sgd.smvRePartition(numParts)
        df = DataFrame(jgdadp.toDF(), self.df.sql_ctx)
        return SmvGroupedData(df, self.keys, jgdadp)

    def smvFillNullWithPrevValue(self, *orderCols):
        """Fill in Null values with "previous" value according to an ordering

            Examples:
                Given DataFrame df representing the table

                +---+---+------+
                | K | T | V    |
                +===+===+======+
                | a | 1 | null |
                +---+---+------+
                | a | 2 | a    |
                +---+---+------+
                | a | 3 | b    |
                +---+---+------+
                | a | 4 | null |
                +---+---+------+

                we can use

                >>> df.smvGroupBy("K").smvFillNullWithPrevValue($"T".asc)("V")

                to produce the result

                +---+---+------+
                | K | T | V    |
                +===+===+======+
                | a | 1 | null |
                +---+---+------+
                | a | 2 | a    |
                +---+---+------+
                | a | 3 | b    |
                +---+---+------+
                | a | 4 | b    |
                +---+---+------+

            Returns:
                (Dataframe): result of fill nulls with previous value
        """
        def __doFill(*valueCols):
            return DataFrame(self.sgd.smvFillNullWithPrevValue(smv_copy_array(self.df._sc, *orderCols), smv_copy_array(self.df._sc, *valueCols)), self.df.sql_ctx)
        return __doFill

    def smvWithTimePanel(self, time_col, start, end):
        """Adding a specified time panel period to the DF

            Args:
                time_col (str): the column name in the data as the event timestamp
                start (panel.PartialTime): could be Day, Month, Week, Quarter, refer
                    the panel package for details
                end (panel.PartialTime): should be the same time type as the "start"
                addMissingTimeWithNull (boolean): Default True. when some PartialTime
                    is missing whether to fill null records

            Returns:
                (DataFrame): a result data frame with keys, and a column with name `smvTime`, and
                    other input columns. Refer the panel package for the potential forms of
                    different PartialTimes.

            Note:
                Since `TimePanel` defines a period of time, if for some group in the data
                there are missing Months (or Quarters), when addMissingTimeWithNull is true,
                this function will add records with non-null keys and
                all possible `smvTime` columns with all other columns null-valued.

            Example:

                Given DataFrame df as

                +---+------------+------+
                | K | TS         | V    |
                +===+============+======+
                | 1 | 2014-01-01 | 1.2  |
                +---+------------+------+
                | 1 | 2014-03-01 | 4.5  |
                +---+------------+------+
                | 1 | 2014-03-25 | 10.3 |
                +---+------------+------+

                after applying

                >>> import smv.panel as P
                >>> df.smvGroupBy("k").smvWithTimePanel("TS", P.Month(2014,1), P.Month(2014, 3))

                the result is

                +---+------------+------+---------+
                | K | TS         | V    | smvTime |
                +===+============+======+=========+
                | 1 | 2014-01-01 | 1.2  | M201401 |
                +---+------------+------+---------+
                | 1 | 2014-03-01 | 4.5  | M201403 |
                +---+------------+------+---------+
                | 1 | 2014-03-25 | 10.3 | M201403 |
                +---+------------+------+---------+
                | 1 | None       | None | M201401 |
                +---+------------+------+---------+
                | 1 | None       | None | M201402 |
                +---+------------+------+---------+
                | 1 | None       | None | M201403 |
                +---+------------+------+---------+

        """
        return DataFrame(self.sgd.smvWithTimePanel(time_col, start, end), self.df.sql_ctx)


    def smvTimePanelAgg(self, time_col, start, end):
        """Apply aggregation on given keys and specified time panel period

            Args:
                time_col (str): the column name in the data as the event timestamp
                start (panel.PartialTime): could be Day, Month, Week, Quarter, refer the panel
                    package for details
                end (panel.PartialTime): should be the same time type as the "start"
                addMissingTimeWithNull (default true) when some PartialTime is missing whether to
                    fill null records

            Both `start` and `end` PartialTime are inclusive.

            Example:

            >>> df.smvGroupBy("K").smvTimePanelAgg("TS", Week(2012, 1, 1), Week(2012, 12, 31))(
                    sum("V").alias("V")
                )

            Returns:
                (DataFrame): a result data frame with keys, and a column with name `smvTime`, and
                    aggregated values. Refer the panel package for the potential forms of different
                    PartialTimes

            When addMissingTimeWithNull is true, the aggregation should be always on the variables
            instead of on literals (should NOT be count(lit(1))).

            Example with on data:

                Given DataFrame df as

                +---+------------+------+
                | K | TS         | V    |
                +===+============+======+
                | 1 | 2012-01-01 | 1.5  |
                +---+------------+------+
                | 1 | 2012-03-01 | 4.5  |
                +---+------------+------+
                | 1 | 2012-07-01 | 7.5  |
                +---+------------+------+
                | 1 | 2012-05-01 | 2.45 |
                +---+------------+------+

                after applying

                >>> import smv.panel as P
                >>> df.smvGroupBy("K").smvTimePanelAgg("TS", P.Quarter(2012, 1), P.Quarter(2012, 2))(
                        sum("V").alias("V")
                    )

                the result is

                +---+---------+------+
                | K | smvTime | V    |
                +===+=========+======+
                | 1 | Q201201 | 6.0  |
                +---+---------+------+
                | 1 | Q201202 | 2.45 |
                +---+---------+------+
        """
        def __doAgg(*aggs):
            return DataFrame(self.sgd.smvTimePanelAgg(time_col, start, end, smv_copy_array(self.df._sc, *aggs)), self.df.sql_ctx)
        return __doAgg

    def smvPercentRank(self, value_cols, ignoreNull=True):
        """Compute the percent rank of a sequence of columns within a group in a given DataFrame.

            Used Spark's `percentRank` window function. The precent rank is defined as
            `R/(N-1)`, where `R` is the base 0 rank, and `N` is the population size. Under
            this definition, min value (R=0) has percent rank `0.0`, and max value has percent
            rank `1.0`.

            For each column for which the percent rank is computed (e.g. "v"), an additional column is
            added to the output, `v_pctrnk`

            All other columns in the input are untouched and propagated to the output.

            Args:
                value_cols (list(str)): columns to calculate percentRank on
                ignoreNull (boolean): if true, null values's percent ranks will be nulls, otherwise,
                    as Spark sort considers null smaller than any value, nulls percent ranks will be
                    zeros. Default true.

            Example:
                >>> df.smvGroupBy('g, 'g2).smvPercentRank(["v1", "v2", "v3"])
        """
        return DataFrame(self.sgd.smvPercentRank(smv_copy_array(self.df._sc, *value_cols), ignoreNull), self.df.sql_ctx)

    def smvQuantile(self, value_cols, bin_num, ignoreNull=True):
        """Compute the quantile bin numbers within a group in a given DataFrame.

            Estimate quantiles and quantile groups given a data with unknown distribution is
            quite arbitrary. There are multiple 'definitions' used in different softwares. Please refer
            https://en.wikipedia.org/wiki/Quantile#Estimating_quantiles_from_a_sample
            for details.

            `smvQuantile` calculated from Spark's `percentRank`. The algorithm is equavalent to the
            one labled as `R-7, Excel, SciPy-(1,1), Maple-6` in above wikipedia page. Please note it
            is slight different from SAS's default algorithm (labled as SAS-5).

            Returned quantile bin numbers are 1 based. For example when `bin_num=10`, returned values are
            integers from 1 to 10, inclusively.

            For each column for which the quantile is computed (e.g. "v"), an additional column is added to
            the output, "v_quantile".

            All other columns in the input are untouched and propagated to the output.

            Args:
                value_cols (list(str)): columns to calculate quantiles on
                bin_num (int): number of quantiles, e.g. 4 for quartile, 10 for decile
                ignoreNull (boolean): if true, null values's percent ranks will be nulls, otherwise,
                    as Spark sort considers null smaller than any value, nulls percent ranks will be
                    zeros. Default true.

            Example:
                >>> df.smvGroupBy('g, 'g2).smvQuantile(["v"], 100)
        """
        return DataFrame(self.sgd.smvQuantile(smv_copy_array(self.df._sc, *value_cols), bin_num, ignoreNull), self.df.sql_ctx)

    def smvDecile(self, value_cols, ignoreNull=True):
        """Compute deciles of some columns on a grouped data

            Simply an alias to `smvQuantile(value_cols, 10, ignoreNull)`

            Args:
                value_cols (list(str)): columns to calculate deciles on
                ignoreNull (boolean): if true, null values's percent ranks will be nulls, otherwise,
                    as Spark sort considers null smaller than any value, nulls percent ranks will be
                    zeros. Default true.
        """
        return self.smvQuantile(value_cols, 10, ignoreNull)


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
                >>> joindf.doJoin()

            Returns:
                (DataFrame): result of executing the join operation
        """
        return DataFrame(self.mj.doJoin(dropextra), self.sqlContext)


def _getUnboundMethod(helperCls, oldMethod):
    def newMethod(_oldMethod, self, *args, **kwargs):
        return _oldMethod(helperCls(self), *args, **kwargs)

    return decorator.decorate(oldMethod, newMethod)


def _helpCls(receiverCls, helperCls):
    iscallable = lambda f: hasattr(f, "__call__")
    for name, oldMethod in inspect.getmembers(helperCls, predicate=iscallable):
        # We will use decorator.decorate to ensure that attributes of oldMethod, like
        # docstring and signature, are inherited by newMethod. decorator.decorate
        # won't accept an unbound method, so for Python 2 we extract oldMethod's
        # implementing function __func__. In Python 3, inspect.getmembers will return
        # the implementing functions insead of unbound method - this is due to
        # Python 3's data model.
        try:
            impl = oldMethod.__func__
        except:
            impl = oldMethod
        if not name.startswith("_"):
            newMethod = _getUnboundMethod(helperCls, impl)
            setattr(receiverCls, name, newMethod)


class DataFrameHelper(object):
    def __init__(self, df):
        self.df = df
        self._sc = df._sc
        self._sql_ctx = df.sql_ctx
        self._jdf = df._jdf
        self._jPythonHelper = df._sc._jvm.SmvPythonHelper
        self._jDfHelper = df._sc._jvm.SmvDFHelper(df._jdf)
        self._SchemaMetaOps = SchemaMetaOps(df)

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
        if isinstance(cols[0], Column):
            keys = [ColumnHelper(c).smvGetColName() for c in cols]
        elif is_string(cols[0]):
            keys = list(cols)
        else:
            raise SmvRuntimeError("smvGroupBy does not support type: " + type(cols[0]))

        jSgd = self._jPythonHelper.smvGroupBy(self._jdf, smv_copy_array(self._sc, *cols))
        return SmvGroupedData(self.df, keys, jSgd)

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
        if is_string(key):
            jkey = F.col(key)._jc
        elif isinstance(key, Column):
            jkey = key._jc
        else:
            raise RuntimeError("key parameter must be either a String or a Column")

        jdf = self._jDfHelper.smvHashSample(jkey, rate, seed)

        return DataFrame(jdf, self._sql_ctx)

    def smvJoinByKey(self, other, keys, joinType, isNullSafe=False):
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
                isNullSafe (boolean): if true matches null keys between left and right tables and keep in output. Default False. 

            Example:
                >>> df1.smvJoinByKey(df2, ["k"], "inner")
                >>> df_with_null_key.smvJoinByKey(df2, ["k"], "inner", True)

            Returns:
                (DataFrame): result of the join operation
        """
        jdf = self._jPythonHelper.smvJoinByKey(self._jdf, other._jdf, _to_seq(keys), joinType, isNullSafe)
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
        """Get top N most frequent values in Column col

            Args:
                n (int): maximum number of values
                col (Column): which column to get values from

            Example:

                >>> df.topNValsByFreq(1, col("cid"))

                will return the single most frequent value in the cid column

            Returns:
                (list(object)): most frequent values (type depends on schema)
        """
        topNdf = DataFrame(self._jDfHelper._topNValsByFreq(n, col._jc), self._sql_ctx)
        return [list(r)[0] for r in topNdf.collect()]

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

                >>> df.smvSkewJoinByKey(df2, "inner", [4], "cid")

                will broadcast join the rows of df1 and df2 where col("cid") == "4"
                and join the remaining rows of df1 and df2 without broadcast join.

            Returns:
                (DataFrame): the result of the join operation
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
                >>> df.smvSelectPlus((col("price") * col("count")).alias("amt"))

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

            Above will add `x_` to the beginning of every column name in the `DataFrame`.
            Please note that if the renamed column names over lap with existing columns,
            the method will error out.

            Returns:
                (DataFrame)
        """
        jdf = self._jDfHelper.smvPrefixFieldNames(prefix)
        return DataFrame(jdf, self._sql_ctx)

    def smvDedupByKey(self, *keys):
        """Remove duplicate records from the DataFrame by arbitrarily selecting the first record from a set of records with same primary key or key combo.

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

                >>> df.smvDedupByKey("id")

                output DataFrame:

                +-----+---------+---------+
                | id  | product | Company |
                +=====+=========+=========+
                | 1   | A       | C1      |
                +-----+---------+---------+
                | 2   | B       | C3      |
                +-----+---------+---------+

                >>> df.smvDedupByKey("id", "product")

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
                Same as the `smvDedupByKey` method, we use RDD groupBy in the implementation of this method to make sure we can handle large key space.

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

                >>> df.smvDedupByKeyWithOrder(col("id"))(col("product").desc())

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
                    | ...|   ...  |  ...  |
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

            Returns:
                (DataFrame): the DataFrame with the key and flag columns
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
        return self._SchemaMetaOps.addDesc(*colDescs)

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

            or:

            Returns:
                (list(tuple)): a list of (colName, description) pairs for all columns
        """
        return self._SchemaMetaOps.getDesc(colName)

    def smvRemoveDesc(self, *colNames):
        """Removes description for the given columns from the Dataframe

            Args:
                colNames (\*string): names of columns for which to remove the description

            Example:
                >>> df.smvRemoveDesc("col_a", "col_b")
                >>> df.smvRemoveDesc()

            Returns:
                (DataFrame): the DataFrame with column descriptions removed

            or:

            Returns:
                (DataFrame): the DataFrae with all column descriptions removed
        """
        return self._SchemaMetaOps.removeDesc(*colNames)

    def smvGetLabel(self, colName = None):
        """Returns a list of column label(s)

            Args:
                colName (string):  optional column name for which to get the label.

            Example:
                >>> df.smvGetLabel("col_a")
                >>> df.smvGetLabel()

            Returns:
                (list(string)): a list of label strings of colName, if specified

            or:

            Returns:
                (list(tuple)): a list of (colName, list(labels)) pairs for all columns
        """
        return self._SchemaMetaOps.getLabel(colName)

    def smvLabel(self, colNames, labels):
        """Adds labels to the specified columns

            A column may have multiple labels. Adding the same label twice
            to a column has the same effect as adding that label once.

            For multiple colNames, the same set of labels will be added to all of them.
            When colNames is empty, the set of labels will be added to all columns of the df.
            labels parameters must be non-empty.

            Args:
                labels: (list(string)) a list of label strings to add
                colNames: (list(string)) list of names of columns for which to add labels

            Example:
                >>> df.smvLabel(["col_a", "col_b", "col_c"], ["tag_1", "tag_2"])
                >>> df.smvLabel([], ["tag_1", "tag_2"])

            Returns:
                (DataFrame): the DataFrame with labels added to the specified columns

            or:

            Returns:
                (DataFrame): the DataFrame with labels added to all columns
        """
        return self._SchemaMetaOps.addLabel(colNames, labels)

    def smvRemoveLabel(self, colNames = None, labels = None):
        """Removes labels from the specified columns

            For multiple colNames, the same set of labels will be removed from all of them.
            When colNames is empty, the set of labels will be removed from all columns of the df.
            When labels is empty, all labels will be removed from the given columns.

            If neither columns nor labels are specified, i.e. both parameter lists are empty,
            then all labels are removed from all columns in the data frame, essentially clearing
            the label meta data.

            Args:
                labels: (list(string)) a list of label strings to remove
                colNames: (list(string)) list of names of columns for which to remove labels

            Example:
                >>> df.smvRemoveLabel(["col_a"], ["tag_1"])
                >>> df.smvRemoveLabel()

            Returns:
                (DataFrame): the DataFrame with specified labels removed from the specified columns

            or:

            Returns:
                (DataFrame): the DataFrame with all label meta data cleared
        """
        return self._SchemaMetaOps.removeLabel(colNames, labels)

    def smvWithLabel(self, labels = None):
        """Returns all column names in the data frame that contain all the specified labels
        
            If the labels is empty, returns all unlabeled columns in the data frame.
            Will throw if there are no columns that satisfy the condition.

            Args:
                labels: (list(string)) a list of label strings for the columns to match

            Example:
                >>> df.smvWithLabel(["tag_1", "tag_2"])

            Returns:
                (list(string)): a list of column name strings that match the specified labels
        """
        return self._SchemaMetaOps.colsWithLabel(labels)

    def selectByLabel(self, labels = None):
        """Select columns whose metadata contains the specified labels

            If the labels is empty, returns a DataFrame with all the unlabeled columns.
            Will throw if there are no columns that satisfy the condition.

            Args:
                labels: (list(string)) a list of label strings for the columns to match

            Example:
                >>> df.selectByLabel(["tag_1"])

            Returns:
                (DataFrame): the DataFrame with the selected columns
        """
        return self.df.select(self.smvWithLabel(labels))

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
        if is_string(keys):
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

            The resulting output is printed out

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

    def smvDupeCheck(self, keys, n=10000):
        """For a given list of potential keys, check for duplicated records with the number of duplications and all the columns.

            Null values are allowed in the potential keys, so duplication on Null valued keys will also be reported.

            Args:
                keys (list(string)): the key column list which the duplicate check applied
                n (integer): number of rows from input data for checking duplications, defaults to 10000

            Returns:
                (DataFrame): returns key columns + "_N" + the rest columns for the records with more key duplication records, 
                    where "_N" has the count of duplications of the key values of that record
        """
        dfTopN = self.df.limit(n).cache()

        res = dfTopN.groupBy(*keys)\
            .agg(F.count(F.lit(1)).alias('_N'))\
            .where(F.col('_N') > 1)\
            .smvJoinByKey(dfTopN, keys, 'inner', True)\
            .orderBy(*keys)

        dfTopN.unpersist()
        return res

    def smvDumpDF(self):
        """Dump the schema and data of given df to screen for debugging purposes

            Similar to `show()` method of DF from Spark 1.3, although the format is slightly different.  This function's format is more convenient for us and hence has remained.

            Example:
                >>> df.smvDumpDF()

            Returns:
                (None)
        """
        self._println(self._jDfHelper._smvDumpDF())

class ColumnHelper(object):
    def __init__(self, col):
        self.col = col
        self._jc = col._jc
        self._jvm = _sparkContext()._jvm
        self._jPythonHelper = self._jvm.SmvPythonHelper
        self._jColumnHelper = self._jvm.ColumnHelper(self._jc)

    def smvGetColName(self):
        """Returns the name of a Column as a sting

            Example:
            >>> df.a.smvGetColName()

            Returns:
                (str)
        """
        return self._jColumnHelper.getName()

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
                (Column): IntegerType. Day of week component as integer (range 1-7, 1 being Monday), or null if input column is null
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
        """Add N days to `Timestamp` or `Date` column

            Args:
                delta (int or Column): the number of days to add

            Example:
                >>> df.select(col("dob").smvPlusDays(3))

            Returns:
                (Column): TimestampType. The incremented Timestamp, or null if input is null.
                    **Note** even if the input is DateType, the output is TimestampType

            Please note that although Spark's `date_add` function does the similar
            thing, they are actually different.

            - Both can act on both `Timestamp` and `Date` types
            - `smvPlusDays` always returns `Timestamp`, while `F.date_add` always returns
              `Date`
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
        """Add N weeks to `Timestamp` or `Date` column

            Args:
                delta (int or Column): the number of weeks to add

            Example:
                >>> df.select(col("dob").smvPlusWeeks(3))

            Returns:
                (Column): TimestampType. The incremented Timestamp, or null if input is null.
                    **Note** even if the input is DateType, the output is TimestampType
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
        """Add N months to `Timestamp` or `Date` column

            Args:
                delta (int or Column): the number of months to add

            Note:
                The calculation will do its best to only change the month field retaining the same day of month. However, in certain circumstances, it may be necessary to alter smaller fields. For example, 2007-03-31 plus one month cannot result in 2007-04-31, so the day of month is adjusted to 2007-04-30.

            Example:
                >>> df.select(col("dob").smvPlusMonths(3))

            Returns:
                (Column): TimestampType. The incremented Timestamp, or null if input is null.
                    **Note** even if the input is DateType, the output is TimestampType

            Please note that although Spark's `add_months` function does the similar
            thing, they are actually different.

            - Both can act on both `Timestamp` and `Date` types
            - `smvPlusMonths` always returns `Timestamp`, while `F.add_months` always returns
              `Date`
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
        """Add N years to `Timestamp` or `Date` column

            Args:
                delta (int or Column): the number of years to add

            Example:
                >>> df.select(col("dob").smvPlusYears(3))

            Returns:
                (Column): TimestampType. The incremented Timestamp, or null if input is null.
                    **Note** even if the input is DateType, the output is TimestampType
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

    def smvTimestampToStr(self, timezone, fmt):
        """Build a string from a timestamp and timezone

            Args:
                timezone (string or Column): the timezone follows the rules in 
                    https://www.joda.org/joda-time/apidocs/org/joda/time/DateTimeZone.html#forID-java.lang.String-
                    It can be a string like "America/Los_Angeles" or "+1000". If it is null, use current system time zone.
                fmt (string): the format is the same as the Java `Date` format

            Example:
                >>> df.select(col("ts").smvTimestampToStr("America/Los_Angeles","yyyy-MM-dd HH:mm:ss"))

            Returns:
                (Column): StringType. The converted String with given format
        """
        if is_string(timezone):
            jtimezone = timezone
        elif isinstance(timezone, Column):
            jtimezone = timezone._jc
        else:
            raise RuntimeError("timezone parameter must be either an string or a Column")
        jc = self._jColumnHelper.smvTimestampToStr(jtimezone, fmt)
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

    def smvTimeToType(self):
        """smvTime helper to convert `smvTime` column to time type string

            Example `smvTime` values (as String): "Q201301", "M201512", "D20141201"
            Example output type "quarter", "month", "day"
        """
        jc = self._jColumnHelper.smvTimeToType()
        return Column(jc)

    def smvTimeToIndex(self):
        """smvTime helper to convert `smvTime` column to time index integer

            Example `smvTime` values (as String): "Q201301", "M201512", "D20141201"
            Example output 172, 551, 16405 (# of quarters, months, and days from 19700101)
        """
        jc = self._jColumnHelper.smvTimeToIndex()
        return Column(jc)

    def smvTimeToLabel(self):
        """smvTime helper to convert `smvTime` column to human readable form

             Example `smvTime` values (as String): "Q201301", "M201512", "D20141201"
             Example output "2013-Q1", "2015-12", "2014-12-01"
        """
        jc = self._jColumnHelper.smvTimeToLabel()
        return Column(jc)

    def smvTimeToTimestamp(self):
        """smvTime helper to convert `smvTime` column to a timestamp at the beginning of
            the given time pireod.

             Example `smvTime` values (as String): "Q201301", "M201512", "D20141201"
             Example output "2013-01-01 00:00:00.0", "2015-12-01 00:00:00.0", "2014-12-01 00:00:00.0"
        """
        jc = self._jColumnHelper.smvTimeToTimestamp()
        return Column(jc)

    def smvArrayFlatten(self, elemType):
        """smvArrayFlatten helper applies flatten operation on an Array of Array
            column.

            Example:
                >>> df.select(col('arrayOfArrayOfStr').smvArrayFlatten(StringType()))

            Args:
                elemType (DataType or DataFram): array element's data type,
                    in object form or the DataFrame to infer the
                    element data type
        """
        if(isinstance(elemType, DataType)):
            elemTypeJson = elemType.json()
        elif(isinstance(elemType, DataFrame)):
            elemTypeJson = elemType.select(self.col)\
                .schema.fields[0].dataType.elementType.elementType.json()
        else:
            raise SmvRuntimeError("smvArrayFlatten does not support type: {}".format(type(elemType)))

        jc = self._jColumnHelper.smvArrayFlatten(elemTypeJson)
        return Column(jc)



# Initialize DataFrame and Column with helper methods. Called by SmvApp.
def init_helpers():
    _helpCls(Column, ColumnHelper)
    _helpCls(DataFrame, DataFrameHelper)
