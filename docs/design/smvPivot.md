# SMV 1.3 Pivot Interface

## smvPivot on DF
smvPivot on DF will output 1 record per input record, and all input fields are kept.

Client code looks like
```scala
   df.smvPivot(Seq("month", "product"))("count")("5_14_A", "5_14_B", "6_14_A", "6_14_B")
```
 
Input
```
 | id  | month | product | count |
 | --- | ----- | ------- | ----- |
 | 1   | 5/14  |   A     |   100 |
 | 1   | 6/14  |   B     |   200 |
 | 1   | 5/14  |   B     |   300 |
```
 
Output
```
Input
```
 | id  | month | product | count |
 | --- | ----- | ------- | ----- |
 | 1   | 5/14  |   A     |   100 |
 | 1   | 6/14  |   B     |   200 |
 | 1   | 5/14  |   B     |   300 |
```
 
``` 

## smvPivot on GD

```scala
df.groupBy("id").smvPivot(
    Seq("month", "product"))(
    "count")(
    "5_14_A", "5_14_B", "6_14_A", "6_14_B")
```

Output
```
 | id  | count_5_14_A | count_5_14_B | count_6_14_A | count_6_14_B |
 | --- | ------------ | ------------ | ------------ | ------------ |
 | 1   | 100          | NULL         | NULL         | NULL         |
 | 1   | NULL         | NULL         | NULL         | 200          |
 | 1   | NULL         | 300          | NULL         | NULL         |
```

Content-wise returns the similar thing as the DataFrame version without unspecified columns. Also has 1-1 map between input 
and output. But the output of it is a GroupedData object (actually SmvGroupedData), so you can do
```scala
df.groupBy('id).smvPivot(...)(...)(...).sum("count_5_14_A", "count_6_14_B")
```
or other aggregate functions with ```agg```.

A convienience function is 
```scala
df.groupBy("id").smvPivotSum(Seq("month", "product"))("count")("5_14_A", "5_14_B", "6_14_A", "6_14_B")
```
It will sum on all derived columns.

Output
```
 | id  | count_5_14_A | count_5_14_B | count_6_14_A | count_6_14_B |
 | --- | ------------ | ------------ | ------------ | ------------ |
 | 1   | 100          | 300          | NULL         | 200          |
```
