# SMV Python Porting Progress Tracking

## DataFrame Helpers
  * [x] `smvSelectPlus((col("a") + 1).alias("b"))`
  * [x] `smvSelectMinus("a")`
  * [x] `smvJoinByKey(df2, ["k1"], "inner")`
  * [x] `smvHashSample("k", 0.02)`
  * [x] `smvDedupByKey("k1", "k2")`
  * [x] `smvDedupByKeyWithOrder(["k1", "k2"], [col("t").desc()])`
  * [x] `smvRenameField(("o", "n"), ("o2", "n2"))`
  * [x] `smvUnion(df2, df3)`
  * [x] `smvExpandStruct("c1", "c2")`
  * [x] `smvGroupBy("k1", "k2")`

## SmvGroupedData Helper
  * [x] `smvTopNRecs(10, col("t").desc(), col("a").asc())`
  * [x] `smvPivotSum(["city"], ["pop"], ["ny", "la", "sd", "sf"])`

## DataFrame Helper in Shell
  * [x] `smvEdd()`
  * [x] `smvHist("k")`
  * [x] `smvConcatHist(["k1", "k2"])`
  * [x] `smvFreqHist("k")`
  * [x] `smvCountHist("k")`
  * [x] `smvBinHist(("k", 100.0))`

## Column Helpers
  * [x] `smvStrToTimestamp("yyyyMMdd")`

**Note** To keep an eye on the progress, please check `python/smv.py`.
