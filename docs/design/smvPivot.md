# SMV 1.3 Pivot Interface

## smvPivot on DF

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
 | id  | month | product | count | count_5_14_A | count_5_14_B | count_6_14_A | count_6_14_B |
 | --- | ----- | ------- | ----- | ------------ | ------------ | ------------ | ------------ |
 | 1   | 5/14  |   A     |   100 | 100          | NULL         | NULL         | NULL         |
 | 1   | 6/14  |   B     |   200 | NULL         | NULL         | NULL         | 200          |
 | 1   | 5/14  |   B     |   300 | NULL         | 300          | NULL         | NULL         |
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

Content-wise returns the similar thing as above, but it's a GroupedData object, so you can do
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
