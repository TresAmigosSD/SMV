# SMV Python Porting Progress Tracking

## Framework
  * [x] SmvPyCsvFile
  * [x] SmvPyHiveTable
  * [x] SmvPyModule
  * [x] SmvPyOutput
  * [ ] SmvPyModuleLink

## DataFrame Helpers
  * [x] `smvSelectPlus((col("a") + 1).alias("b"))`
  * [x] `smvSelectMinus("a")`
  * [x] `smvJoinByKey(df2, ["k1"], "inner")`
  * [x] `smvJoinMultipleByKey(["k1"], "inner")`
  * [x] `smvHashSample("k", 0.02)`
  * [x] `smvDedupByKey("k1", "k2")`
  * [x] `smvDedupByKeyWithOrder("k1", "k2")(col("t").desc())`
  * [x] `smvRenameField(("o", "n"), ("o2", "n2"))`
  * [x] `smvUnion(df2, df3)`
  * [x] `smvExpandStruct("c1", "c2")`
  * [x] `smvGroupBy("k1", "k2")`
  * [x] `smvDiscoverPK()`
  * [x] `smvDumpDF()`
  * [x] `smvUnpivot("colx", "coly", "colx")`

## SmvGroupedData Helper
  * [x] `smvTopNRecs(10, col("t").desc(), col("a").asc())`
  * [x] `smvPivotSum(["city"], ["pop"], ["ny", "la", "sd", "sf"])`
  * [x] `smvPivotCoalesce(["city"], ["pop"], ["ny", "la", "sd", "sf"])`
  * [x] `smvFillNullWithPreValue(col("T").asc())("V")`

## DataFrame Helper in Shell
  * [x] `smvEdd()`
  * [x] `smvHist("k")`
  * [x] `smvConcatHist(["k1", "k2"])`
  * [x] `smvFreqHist("k")`
  * [x] `smvCountHist("k")`
  * [x] `smvBinHist(("k", 100.0))`
  * [x] `smvEddCompare(otherDf)`

## Column Helpers
  * [x] `smvIsAllIn("value 1", "value 2")`
  * [x] `smvIsAnyIn("value 1", "value 2")`
  * [x] `smvStrToTimestamp("yyyyMMdd")`
  * [x] `smvMonth`
  * [x] `smvYear`
  * [x] `smvQuarter`
  * [x] `smvDayOfMonth`
  * [x] `smvDayOfYear`
  * [x] `smvHour`
  * [x] `smvPlusDays`
  * [x] `smvPlusWeeks`
  * [x] `smvPlusMonths`
  * [x] `smvPlusYears`
  * [x] `smvDay70`
  * [x] `smvMonth70`
  * [x] `smvNullSub`

## Misc in Shell
  * [x] `pdf("com.mycomp.myapp.mypythonfile.mymodule")`
  * [x] `ddf("com.mycomp.myapp.myscalamodule")`
  * [x] `openHive("hiveDBname.hivetableName")`
  * [x] `openCsv("path/to/csvFile.csv")`
  * [x] `app.createDF("a:String", "a;b;c")`

**Note** To keep an eye on the progress, please check `python/smv.py`.
