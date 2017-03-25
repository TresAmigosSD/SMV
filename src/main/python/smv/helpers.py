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
    """Wrapper around the Scala SmvGroupedData"""
    def __init__(self, df, sgd):
        self.df = df
        self.sgd = sgd

    def smvTopNRecs(self, maxElems, *cols):
        return DataFrame(self.sgd.smvTopNRecs(maxElems, smv_copy_array(self.df._sc, *cols)), self.df.sql_ctx)

    def smvPivotSum(self, pivotCols, valueCols, baseOutput):
        """Perform a normal SmvPivot using the pivot and value columns, followed by summing all the output.
        TODO: docstring parameters and return values
        pivotCols: list of list of strings
        valueCols: list of strings
        baseOutput: list of strings"""
        return DataFrame(self.sgd.smvPivotSum(smv_copy_array(self.df._sc, *pivotCols), smv_copy_array(self.df._sc, *valueCols), smv_copy_array(self.df._sc, *baseOutput)), self.df.sql_ctx)

    def smvPivotCoalesce(self, pivotCols, valueCols, baseOutput):
        """Perform a normal SmvPivot using the pivot and value columns, followed by coalescing all the output.
        pivotCols: list of list of strings
        valueCols: list of strings
        baseOutput: list of strings"""
        return DataFrame(self.sgd.smvPivotCoalesce(smv_copy_array(self.df._sc, *pivotCols), smv_copy_array(self.df._sc, *valueCols), smv_copy_array(self.df._sc, *baseOutput)), self.df.sql_ctx)

    def smvFillNullWithPrevValue(self, *orderCols):
        """Fill in Null values with "previous" value according to an ordering
           * Example:
           * Input:
           * K, T, V
           * a, 1, null
           * a, 2, a
           * a, 3, b
           * a, 4, null
           *
           * df.smvGroupBy("K").smvFillNullWithPrevValue($"T".asc)("V")
           *
           * Output:
           * K, T, V
           * a, 1, null
           * a, 2, a
           * a, 3, b
           * a, 4, b
           *
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
        return SmvMultiJoin(self.sqlContext, self.mj.joinWith(df._jdf, postfix, jointype))

    def doJoin(self, dropextra = False):
        return DataFrame(self.mj.doJoin(dropextra), self.sqlContext)

helper = lambda df: df._sc._jvm.SmvPythonHelper
def dfhelper(df):
    return _sparkContext()._jvm.SmvDFHelper(df._jdf)

def colhelper(c):
    return _sparkContext()._jvm.ColumnHelper(c._jc)

DataFrame.smvExpandStruct = lambda df, *cols: DataFrame(helper(df).smvExpandStruct(df._jdf, smv_copy_array(df._sc, *cols)), df.sql_ctx)

# TODO can we port this without going through the proxy?
DataFrame.smvGroupBy = lambda df, *cols: SmvGroupedData(df, helper(df).smvGroupBy(df._jdf, smv_copy_array(df._sc, *cols)))

def __smvHashSample(df, key, rate=0.01, seed=23):
    if (isinstance(key, basestring)):
        jkey = col(key)._jc
    elif (isinstance(key, Column)):
        jkey = key._jc
    else:
        raise RuntimeError("key parameter must be either a String or a Column")
    return DataFrame(dfhelper(df).smvHashSample(jkey, rate, seed), df.sql_ctx)
DataFrame.smvHashSample = __smvHashSample

# FIXME py4j method resolution with null argument can fail, so we
# temporarily remove the trailing parameters till we can find a
# workaround
DataFrame.smvJoinByKey = lambda df, other, keys, joinType: DataFrame(helper(df).smvJoinByKey(df._jdf, other._jdf, _to_seq(keys), joinType), df.sql_ctx)

DataFrame.smvJoinMultipleByKey = lambda df, keys, joinType = 'inner': SmvMultiJoin(df.sql_ctx, helper(df).smvJoinMultipleByKey(df._jdf, smv_copy_array(df._sc, *keys), joinType))

DataFrame.smvSelectMinus = lambda df, *cols: DataFrame(helper(df).smvSelectMinus(df._jdf, smv_copy_array(df._sc, *cols)), df.sql_ctx)

DataFrame.smvSelectPlus = lambda df, *cols: DataFrame(dfhelper(df).smvSelectPlus(_to_seq(cols, _jcol)), df.sql_ctx)

DataFrame.smvDedupByKey = lambda df, *keys: DataFrame(helper(df).smvDedupByKey(df._jdf, smv_copy_array(df._sc, *keys)), df.sql_ctx)

def __smvDedupByKeyWithOrder(df, *keys):
    def _withOrder(*orderCols):
        return DataFrame(helper(df).smvDedupByKeyWithOrder(df._jdf, smv_copy_array(df._sc, *keys), smv_copy_array(df._sc, *orderCols)), df.sql_ctx)
    return _withOrder
DataFrame.smvDedupByKeyWithOrder = __smvDedupByKeyWithOrder

DataFrame.smvUnion = lambda df, *dfothers: DataFrame(dfhelper(df).smvUnion(_to_seq(dfothers, _jdf)), df.sql_ctx)

DataFrame.smvRenameField = lambda df, *namePairs: DataFrame(helper(df).smvRenameField(df._jdf, smv_copy_array(df._sc, *namePairs)), df.sql_ctx)

DataFrame.smvUnpivot = lambda df, *cols: DataFrame(dfhelper(df).smvUnpivot(_to_seq(cols)), df.sql_ctx)

DataFrame.smvUnpivotRegex = lambda df, colNameFn, indexColName, *cols: DataFrame(dfhelper(df).smvUnpivotRegex(_to_seq(cols), colNameFn, indexColName), df.sql_ctx)

DataFrame.smvExportCsv = lambda df, path, n=None: dfhelper(df).smvExportCsv(path, n)

def __smvOverlapCheck(df, keyColName):
    def _check(*dfothers):
        return DataFrame(helper(df).smvOverlapCheck(df._jdf, keyColName, smv_copy_array(df._sc, *dfothers)), df.sql_ctx)
    return _check
DataFrame.smvOverlapCheck = __smvOverlapCheck

#############################################
# DfHelpers which print to STDOUT
# Scala side which print to STDOUT will not work on Jupyter. Have to pass the string to python side then print to stdout
#############################################
println = lambda str: sys.stdout.write(str + "\n")

def printFile(f, str):
    tgt = open(f, "w")
    tgt.write(str + "\n")
    tgt.close()

DataFrame.peek = lambda df, pos = 1, colRegex = ".*": println(helper(df).peekStr(df._jdf, pos, colRegex))
DataFrame.peekSave = lambda df, path, pos = 1, colRegex = ".*": printFile(path, helper(df).peekStr(df._jdf, pos, colRegex))

def _smvEdd(df, *cols): return dfhelper(df)._smvEdd(_to_seq(cols))
def _smvHist(df, *cols): return dfhelper(df)._smvHist(_to_seq(cols))
def _smvConcatHist(df, cols): return helper(df).smvConcatHist(df._jdf, smv_copy_array(df._sc, *cols))
def _smvFreqHist(df, *cols): return dfhelper(df)._smvFreqHist(_to_seq(cols))
def _smvCountHist(df, keys, binSize): return dfhelper(df)._smvCountHist(_to_seq(keys), binSize)
def _smvBinHist(df, *colWithBin):
    for elem in colWithBin:
        assert type(elem) is tuple, "smvBinHist takes a list of tuple(string, double) as paraeter"
        assert len(elem) == 2, "smvBinHist takes a list of tuple(string, double) as parameter"
    insureDouble = map(lambda t: (t[0], t[1] * 1.0), colWithBin)
    return helper(df).smvBinHist(df._jdf, smv_copy_array(df._sc, *insureDouble))

def _smvEddCompare(df, df2, ignoreColName): return dfhelper(df)._smvEddCompare(df2._jdf, ignoreColName)

DataFrame.smvEdd = lambda df, *cols: println(_smvEdd(df, *cols))
DataFrame.smvHist = lambda df, *cols: println(_smvHist(df, *cols))
DataFrame.smvConcatHist = lambda df, cols: println(_smvConcatHist(df, cols))
DataFrame.smvFreqHist = lambda df, *cols: println(_smvFreqHist(df, *cols))
DataFrame.smvEddCompare = lambda df, df2, ignoreColName=False: println(_smvEddCompare(df, df2, ignoreColName))

def __smvCountHistFn(df, keys, binSize = 1):
    if (isinstance(keys, basestring)):
        return println(_smvCountHist(df, [keys], binSize))
    else:
        return println(_smvCountHist(df, keys, binSize))
DataFrame.smvCountHist = __smvCountHistFn

DataFrame.smvBinHist = lambda df, *colWithBin: println(_smvBinHist(df, *colWithBin))

def __smvDiscoverPK(df, n):
    res = helper(df).smvDiscoverPK(df._jdf, n)
    println("[{}], {}".format(", ".join(map(str, res._1())), res._2()))

DataFrame.smvDiscoverPK = lambda df, n=10000: __smvDiscoverPK(df, n)

DataFrame.smvDumpDF = lambda df: println(dfhelper(df)._smvDumpDF())

#############################################
# ColumnHelper methods:
#############################################

# SmvPythonHelper is necessary as frontend to generic Scala functions
Column.smvIsAllIn = lambda c, *vals: Column(_sparkContext()._jvm.SmvPythonHelper.smvIsAllIn(c._jc, _to_seq(vals)))
Column.smvIsAnyIn = lambda c, *vals: Column(_sparkContext()._jvm.SmvPythonHelper.smvIsAnyIn(c._jc, _to_seq(vals)))

Column.smvMonth      = lambda c: Column(colhelper(c).smvMonth())
Column.smvYear       = lambda c: Column(colhelper(c).smvYear())
Column.smvQuarter    = lambda c: Column(colhelper(c).smvQuarter())
Column.smvDayOfMonth = lambda c: Column(colhelper(c).smvDayOfMonth())
Column.smvDayOfWeek  = lambda c: Column(colhelper(c).smvDayOfWeek())
Column.smvHour       = lambda c: Column(colhelper(c).smvHour())

Column.smvPlusDays   = lambda c, delta: Column(colhelper(c).smvPlusDays(delta))
Column.smvPlusWeeks  = lambda c, delta: Column(colhelper(c).smvPlusWeeks(delta))
Column.smvPlusMonths = lambda c, delta: Column(colhelper(c).smvPlusMonths(delta))
Column.smvPlusYears  = lambda c, delta: Column(colhelper(c).smvPlusYears(delta))

Column.smvDay70 = lambda c: Column(colhelper(c).smvDay70())
Column.smvMonth70 = lambda c: Column(colhelper(c).smvMonth70())

Column.smvStrToTimestamp = lambda c, fmt: Column(colhelper(c).smvStrToTimestamp(fmt))
